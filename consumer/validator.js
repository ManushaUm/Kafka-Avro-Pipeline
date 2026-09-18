const { PermanentError } = require("./errors");

/**
 * Validates order schema and business invariants.
 *
 * Rules:
 * - orderId must be present and non-empty.
 * - product must be present and non-empty.
 * - price must be a valid number and strictly greater than 0.
 *
 * Any violation is a PermanentError that should bypass the retry engine
 * and be dispatched straight to the Dead Letter Queue.
 */
function validateOrder(order) {
  if (!order || typeof order !== "object") {
    throw new PermanentError("Malformed payload: order is not an object");
  }

  if (!order.orderId || typeof order.orderId !== "string" || order.orderId.trim() === "") {
    throw new PermanentError(`Missing or invalid orderId: received ${JSON.stringify(order.orderId)}`);
  }

  if (!order.product || typeof order.product !== "string" || order.product.trim() === "") {
    throw new PermanentError(`Missing or invalid product for order ${order.orderId}: received ${JSON.stringify(order.product)}`);
  }

  if (typeof order.price !== "number" || isNaN(order.price)) {
    throw new PermanentError(`Invalid price type for order ${order.orderId}: received ${typeof order.price}`);
  }

  if (order.price <= 0) {
    throw new PermanentError(`Business rule violation: price must be positive, got ${order.price} for order ${order.orderId}`);
  }

  return true;
}

module.exports = {
  validateOrder,
};
