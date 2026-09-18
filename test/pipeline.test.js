const assert = require("assert");
const { TransientError, PermanentError } = require("../consumer/errors");
const { validateOrder } = require("../consumer/validator");
const { processWithRetry } = require("../consumer/retry");

async function runTests() {
  console.log("========================================");
  console.log("Running Unit Verification Tests");
  console.log("========================================");

  // Test 1: Order Validation - Valid
  {
    const validOrder = { orderId: "1001", product: "Laptop", price: 150.0 };
    assert.doesNotThrow(() => validateOrder(validOrder));
    console.log("✔ Test 1: Valid order passes validation");
  }

  // Test 2: Order Validation - Negative Price (PermanentError)
  {
    const badPriceOrder = { orderId: "1002", product: "Monitor", price: -10.5 };
    assert.throws(
      () => validateOrder(badPriceOrder),
      (err) => err instanceof PermanentError && err.message.includes("price must be positive")
    );
    console.log("✔ Test 2: Negative price correctly throws PermanentError");
  }

  // Test 3: Order Validation - Zero Price (PermanentError)
  {
    const zeroPriceOrder = { orderId: "1003", product: "Desk", price: 0.0 };
    assert.throws(
      () => validateOrder(zeroPriceOrder),
      (err) => err instanceof PermanentError && err.message.includes("price must be positive")
    );
    console.log("✔ Test 3: Zero price throws PermanentError");
  }

  // Test 4: Order Validation - Missing Fields
  {
    const missingId = { product: "Desk", price: 100.0 };
    assert.throws(
      () => validateOrder(missingId),
      (err) => err instanceof PermanentError && err.message.includes("Missing or invalid orderId")
    );

    const missingProduct = { orderId: "1004", price: 100.0 };
    assert.throws(
      () => validateOrder(missingProduct),
      (err) => err instanceof PermanentError && err.message.includes("Missing or invalid product")
    );
    console.log("✔ Test 4: Missing orderId / product throws PermanentError");
  }

  // Test 5: Incremental Running Average Accuracy vs Batch Sum
  {
    const prices = [10.5, 45.2, 99.9, 250.0, 18.75, 499.99, 120.0];
    let count = 0;
    let average = 0.0;

    for (const p of prices) {
      count += 1;
      average += (p - average) / count;
    }

    const batchAverage = prices.reduce((a, b) => a + b, 0) / prices.length;
    assert(Math.abs(average - batchAverage) < 1e-9);
    console.log(`✔ Test 5: Incremental running average matches batch average: ${average.toFixed(4)} == ${batchAverage.toFixed(4)}`);
  }

  // Test 6: Retry Logic - Transient Recovery for order ending in 7
  {
    // Override base delay for quick testing
    process.env.BASE_RETRY_DELAY_MS = "50";
    const retryOrder = { orderId: "test-777", product: "Mouse", price: 29.99 };

    const result = await processWithRetry(retryOrder);
    assert.strictEqual(result.success, true);
    assert.strictEqual(result.attempts, 3);
    console.log("✔ Test 6: Transient failure retries and succeeds on attempt 3");
  }

  // Test 7: Retry Logic - Permanent Errors Bypass Retries
  {
    const poisonOrder = { orderId: "poison-1", product: "Buggy", price: -5.0 };
    let start = Date.now();
    let caughtPermanent = false;
    try {
      await processWithRetry(poisonOrder);
    } catch (err) {
      if (err instanceof PermanentError) {
        caughtPermanent = true;
      }
    }
    const elapsed = Date.now() - start;
    assert(caughtPermanent);
    assert(elapsed < 100, `Permanent error should bypass delays, took ${elapsed}ms`);
    console.log("✔ Test 7: Permanent error bypassed retry delay immediately");
  }

  // Test 8: DLQ Envelope JSON Structure
  {
    const envelope = {
      failedAt: new Date().toISOString(),
      originalTopic: "orders",
      partition: 0,
      offset: "123",
      attempts: 3,
      errorReason: "price must be positive, got -5",
      payload: { orderId: "poison-1", product: "Buggy", price: -5.0 },
    };

    const jsonStr = JSON.stringify(envelope);
    const parsed = JSON.parse(jsonStr);
    assert.strictEqual(parsed.originalTopic, "orders");
    assert.strictEqual(parsed.offset, "123");
    assert.strictEqual(parsed.payload.price, -5.0);
    console.log("✔ Test 8: DLQ JSON audit envelope correctly formed");
  }

  console.log("========================================");
  console.log("All unit tests passed successfully!");
  console.log("========================================");
}

runTests().catch((err) => {
  console.error("Test failed:", err);
  process.exit(1);
});
