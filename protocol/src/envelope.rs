use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::sign;

/// Every message on the wire, in both directions. `payload` is kept as raw,
/// unparsed JSON so the signature always covers the exact bytes that were
/// actually transmitted — re-serializing a parsed `Value` could reorder
/// fields or change whitespace and silently break verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub kind: String,
    pub timestamp: i64,
    pub nonce: String,
    pub payload: Box<RawValue>,
    pub sig: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("serialize payload: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// How far a timestamp may drift from "now" (either direction) and still be
/// accepted. Chosen to comfortably absorb clock skew between the panel and
/// a node while still bounding the replay window tightly.
pub const MAX_CLOCK_SKEW_SECS: i64 = 30;

impl Envelope {
    /// Builds and signs a new envelope carrying `payload`, using `secret` as
    /// the HMAC key (the node's raw token).
    pub fn signed<T: Serialize>(
        secret: &[u8],
        kind: &str,
        payload: &T,
    ) -> Result<Self, EnvelopeError> {
        let payload_bytes = serde_json::to_vec(payload)?;
        let timestamp = now_unix();
        let nonce = random_nonce();
        let sig = sign::sign(secret, kind, timestamp, &nonce, &payload_bytes);
        let raw = RawValue::from_string(
            String::from_utf8(payload_bytes).expect("serde_json emits valid UTF-8"),
        )?;

        Ok(Envelope {
            kind: kind.to_string(),
            timestamp,
            nonce,
            payload: raw,
            sig,
        })
    }

    /// Verifies this envelope's signature and timestamp freshness against
    /// `secret`. Does **not** check nonce uniqueness — that's stateful and
    /// belongs to the caller (a short-lived seen-nonce cache per
    /// connection/process), not this otherwise-pure type.
    pub fn verify(&self, secret: &[u8]) -> bool {
        if !self.timestamp_is_fresh() {
            return false;
        }
        sign::verify(
            secret,
            &self.kind,
            self.timestamp,
            &self.nonce,
            self.payload.get().as_bytes(),
            &self.sig,
        )
    }

    pub fn timestamp_is_fresh(&self) -> bool {
        (now_unix() - self.timestamp).abs() <= MAX_CLOCK_SKEW_SECS
    }

    pub fn decode_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.payload.get())
    }
}

fn random_nonce() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Dummy {
        a: i32,
        b: String,
    }

    #[test]
    fn signed_envelope_verifies_with_correct_secret() {
        let payload = Dummy {
            a: 1,
            b: "hi".into(),
        };
        let env = Envelope::signed(b"secret", "event", &payload).unwrap();
        assert!(env.verify(b"secret"));
    }

    #[test]
    fn signed_envelope_fails_with_wrong_secret() {
        let payload = Dummy {
            a: 1,
            b: "hi".into(),
        };
        let env = Envelope::signed(b"secret", "event", &payload).unwrap();
        assert!(!env.verify(b"wrong-secret"));
    }

    #[test]
    fn decode_payload_round_trips() {
        let payload = Dummy {
            a: 42,
            b: "hello".into(),
        };
        let env = Envelope::signed(b"secret", "event", &payload).unwrap();
        let decoded: Dummy = env.decode_payload().unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn stale_timestamp_fails_verification() {
        let payload = Dummy {
            a: 1,
            b: "hi".into(),
        };
        let mut env = Envelope::signed(b"secret", "event", &payload).unwrap();
        env.timestamp -= MAX_CLOCK_SKEW_SECS + 5;
        // The signature was computed with the original timestamp, so it's
        // now also cryptographically invalid — but even if it were somehow
        // valid, staleness alone must fail verification.
        assert!(!env.verify(b"secret"));
        assert!(!env.timestamp_is_fresh());
    }

    #[test]
    fn two_signed_envelopes_get_distinct_nonces() {
        let payload = Dummy {
            a: 1,
            b: "hi".into(),
        };
        let a = Envelope::signed(b"secret", "event", &payload).unwrap();
        let b = Envelope::signed(b"secret", "event", &payload).unwrap();
        assert_ne!(a.nonce, b.nonce);
    }

    #[test]
    fn wire_round_trip_via_json() {
        let payload = Dummy {
            a: 7,
            b: "wire".into(),
        };
        let env = Envelope::signed(b"secret", "event", &payload).unwrap();

        let wire = serde_json::to_string(&env).unwrap();
        let parsed: Envelope = serde_json::from_str(&wire).unwrap();

        assert!(parsed.verify(b"secret"));
        let decoded: Dummy = parsed.decode_payload().unwrap();
        assert_eq!(decoded, payload);
    }
}
