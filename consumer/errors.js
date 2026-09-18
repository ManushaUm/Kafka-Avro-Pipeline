/**
 * Error hierarchy for Kafka streaming pipeline.
 *
 * Distinct classes are used to separate recoverable (transient) errors
 * from unrecoverable (permanent) errors.
 */

class TransientError extends Error {
  constructor(message) {
    super(message);
    this.name = "TransientError";
  }
}

class PermanentError extends Error {
  constructor(message) {
    super(message);
    this.name = "PermanentError";
  }
}

module.exports = {
  TransientError,
  PermanentError,
};
