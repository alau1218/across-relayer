import { utils as sdkUtils } from "@across-protocol/sdk";
import {
  config,
  delay,
  disconnectRedisClients,
  getNetworkName,
  getRedisCache,
  Profiler,
  Signer,
  winston,
} from "../utils";
import { Relayer } from "./Relayer";
import { RelayerConfig } from "./RelayerConfig";
import { constructRelayerClients } from "./RelayerClientHelper";
config();
let logger: winston.Logger;

const ACTIVE_RELAYER_EXPIRY = 600; // 10 minutes.
const {
  RUN_IDENTIFIER: runIdentifier,
  BOT_IDENTIFIER: botIdentifier = "across-relayer",
  RELAYER_MAX_STARTUP_DELAY = "120",
} = process.env;

const maxStartupDelay = Number(RELAYER_MAX_STARTUP_DELAY);

export async function runRelayer(_logger: winston.Logger, baseSigner: Signer): Promise<void> {
  const profiler = new Profiler({
    at: "Relayer#run",
    logger: _logger,
  });

  logger = _logger;
  const config = new RelayerConfig(process.env);
  const { externalIndexer, pollingDelay, sendingTransactionsEnabled, sendingSlowRelaysEnabled } = config;

  const loop = pollingDelay > 0;
  let stop = false;
  process.on("SIGHUP", () => {
    logger.debug({
      at: "Relayer#run",
      message: "Received SIGHUP, stopping at end of current loop.",
    });
    stop = true;
  });

  const redis = await getRedisCache(logger);
  let activeRelayerUpdated = false;

  // Explicitly don't log ignoredAddresses because it can be huge and can overwhelm log transports.
  const { ignoredAddresses: _ignoredConfig, ...loggedConfig } = config;
  logger.debug({ at: "Relayer#run", message: "Relayer started 🏃‍♂️"});
  const mark = profiler.start("relayer");
  const relayerClients = await constructRelayerClients(logger, config, baseSigner);
  logger.debug({ at: "Relayer#run", message: "Relayer clients constructed...." });
  const relayer = new Relayer(await baseSigner.getAddress(), logger, relayerClients, config);
  await relayer.init();
  logger.debug({ at: "Relayer#run", message: "Relayer initialized...." });

  const { spokePoolClients } = relayerClients;
  const simulate = !sendingTransactionsEnabled;
  let txnReceipts: { [chainId: number]: Promise<string[]> } = {};

  try {
    for (let run = 1; !stop; ++run) {
      if (loop) {
        logger.debug({ at: "relayer#run", message: `Starting relayer execution loop ${run}.` });
      }
      const tLoopStart = profiler.start("Relayer execution loop");
      const ready = await relayer.update();
      const activeRelayer = redis ? await redis.get(botIdentifier) : undefined;

      // If there is another active relayer, allow up to 120 seconds for this instance to be ready.
      // If this instance can't update, throw an error (for now).
      if (!ready && activeRelayer) {
        if (run * pollingDelay < maxStartupDelay) {
          const runTime = Math.round((performance.now() - tLoopStart.startTime) / 1000);
          const delta = pollingDelay - runTime;
          logger.debug({ at: "Relayer#run", message: `Not ready to relay, waiting ${delta} seconds.` });
          await delay(delta);
          continue;
        }

        const badChains = Object.values(spokePoolClients)
          .filter(({ isUpdated }) => !isUpdated)
          .map(({ chainId }) => getNetworkName(chainId));
        throw new Error(`Unable to start relayer due to chains ${badChains.join(", ")}`);
      }

      // Signal to any existing relayer that a handover is underway, or alternatively
      // check for handover initiated by another (newer) relayer instance.
      if (loop && runIdentifier && redis) {
        if (activeRelayer !== runIdentifier) {
          if (!activeRelayerUpdated) {
            logger.debug({
              at: "Relayer#run",
              message: `Taking over from ${botIdentifier} instance ${activeRelayer}.`,
            });
            await redis.set(botIdentifier, runIdentifier, ACTIVE_RELAYER_EXPIRY);
            activeRelayerUpdated = true;
          } else {
            logger.debug({ at: "Relayer#run", message: `Handing over to ${botIdentifier} instance ${activeRelayer}.` });
            stop = true;
          }
        }
      }

      // Align with Ethereum block clock - pause until the 10th second of an Ethereum slot
      if (!stop) {
        const now = new Date().getTime();
        const slotDuration = 12 * 1000; // 12 seconds per slot in milliseconds
        
        // Post-Merge Ethereum genesis timestamp (Sept 15, 2022 - The Merge)
        // This is in seconds since Unix epoch, so convert to milliseconds
        const genesisTimestamp = 1663220516 * 1000;
        
        // Calculate how many milliseconds have elapsed since genesis
        const msSinceGenesis = now - genesisTimestamp;
        
        // Calculate current position within the slot relative to genesis timing
        const msIntoSlot = msSinceGenesis % slotDuration;
        
        // Target the 10th second (10000ms) of the slot
        const targetTime = 10000;
        
        // Calculate wait time to reach the 10th second of current/next slot
        const msUntilTarget = msIntoSlot <= targetTime 
          ? targetTime - msIntoSlot 
          : slotDuration - msIntoSlot + targetTime;
        
        if (msUntilTarget > 0) {
          logger.debug({
            at: "Relayer#run",
            message: `Aligning with Ethereum slot time. Waiting ${msUntilTarget/1000} seconds to reach 10th second of slot.`,
          });
          await new Promise(resolve => setTimeout(resolve, msUntilTarget));
        }
      }

      if (!stop) {
        txnReceipts = await relayer.checkForUnfilledDepositsAndFill(sendingSlowRelaysEnabled, simulate);
        await relayer.runMaintenance();
      }

      if (!loop) {
        stop = true;
      } else {
        const runTimeMilliseconds = tLoopStart.stop({
          message: "Completed relayer execution loop.",
          loopCount: run,
        });
        if (!stop) {
          const runTime = Math.round(runTimeMilliseconds / 1000);

          // When txns are pending submission, yield execution to ensure they can be submitted.
          const minDelay = Object.values(txnReceipts).length > 0 ? 0.1 : 0;
          const delta = pollingDelay > runTime ? pollingDelay - runTime : minDelay;
          logger.debug({
            at: "relayer#run",
            message: `Waiting ${delta} s before next loop.`,
          });
          await delay(delta);
        }
      }
    }

    // Before exiting, wait for transaction submission to complete.
    for (const [chainId, submission] of Object.entries(txnReceipts)) {
      const [result] = await Promise.allSettled([submission]);
      if (sdkUtils.isPromiseRejected(result)) {
        logger.warn({
          at: "Relayer#runRelayer",
          message: `Failed transaction submission on ${getNetworkName(Number(chainId))}.`,
          reason: result.reason,
        });
      }
    }
  } finally {
    await disconnectRedisClients(logger);

    if (externalIndexer) {
      Object.values(spokePoolClients).map((spokePoolClient) => spokePoolClient.stopWorker());
    }
  }

  mark.stop({ message: "Relayer instance completed." });
}
