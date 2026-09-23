/// Minimal caller-owned worker input. Its idempotency key belongs to this app.
pub type PaymentRequest {
  PaymentRequest(idempotency_key: String, amount: Int)
}

pub type PaymentError {
  PaymentRejected(key: String)
}
