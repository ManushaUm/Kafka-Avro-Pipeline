const { TransientError, PermanentError } = require("./errors");
const { validateOrder } = require("./validator");

const MAX_ATTEMPTS = parseInt(process.env.MAX_RETRY_ATTEMPTS || "3", 10);
const BASE_DELAY_MS = parseInt(process.env.BASE_RETRY_DELAY_MS || "1000", 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Simulated downstream worker service.
 * Contains failure switches for live demonstrations:
 * 1. Orders ending in "7" simulate 2 transient failures (HTTP 503) and succeed on the 3rd.
 * 2. If FAIL_TRANSIENT_ALWAYS=true, all attempts fail to prove DLQ on retry exhaustion.
 */
async function doWork(order, attempt) {
  const forceFailure = process.env.FAIL_TRANSIENT_ALWAYS === "true";
  const isDemonstrationOrder = String(order.orderId).endsWith("7");

  if (forceFailure) {
    throw new TransientError("Forced downstream service timeout (HTTP 503)");
  }

  if (isDemonstrationOrder && attempt < 3) {
    throw new TransientError(
      `Simulated upstream payment gateway timeout (attempt ${attempt})`
    );
  }

  if (isDemonstrationOrder && attempt === 3) {
    console.log(`  [DEMO] Upstream recovered on attempt 3 for order ${order.orderId}!`);
  }

  // Normal simulated async operation (e.g. database commit / ledger entry)
  return true;
}

/**
 * Executes message processing wrapped in exponential backoff retry.
 * Permanent errors (e.g. invalid business rules, bad schema) bypass retry immediately.
 */
async function processWithRetry(order) {
  // Step 1: Validate payload. Permanent errors bypass retry entirely.
  validateOrder(order);

  // Step 2: Retry loop with exponential backoff for transient errors
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await doWork(order, attempt);
      return { success: true, attempts: attempt };
    } catch (err) {
      if (err instanceof TransientError) {
        if (attempt === MAX_ATTEMPTS) {
          throw new PermanentError(
            `Failed after ${MAX_ATTEMPTS} attempts: ${err.message}`
          );
        }

        const delayMs = BASE_DELAY_MS * Math.pow(2, attempt - 1); // 1s, 2s, 4s...
        console.log(
          `  [RETRY] Order ${order.orderId}: Attempt ${attempt}/${MAX_ATTEMPTS} failed (${err.message}). Retrying in ${(delayMs / 1000).toFixed(1)}s...`
        );
        await sleep(delayMs);
      } else {
        // Any unexpected or non-transient error is treated as permanent
        throw err;
      }
    }
  }
}

module.exports = {
  processWithRetry,
  MAX_ATTEMPTS,
  BASE_DELAY_MS,
};
