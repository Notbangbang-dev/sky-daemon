//! HMAC-SHA256 signing/verification for envelopes. The signing key is the
//! node's raw token (see `panel-api`'s `nodes.token` column) — every
//! envelope after `hello` must carry a valid signature, computed over the
//! exact raw bytes of its payload so re-serialization on either side can
//! never desync the signature.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Builds the canonical string that gets HMAC'd: `type.timestamp.nonce.sha256(payload)`.
fn canonical_string(kind: &str, timestamp: i64, nonce: &str, payload_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let payload_hash = hex::encode(hasher.finalize());
    format!("{kind}.{timestamp}.{nonce}.{payload_hash}")
}

/// Returns the hex-encoded HMAC-SHA256 signature for the given fields.
pub fn sign(
    secret: &[u8],
    kind: &str,
    timestamp: i64,
    nonce: &str,
    payload_bytes: &[u8],
) -> String {
    let canonical = canonical_string(kind, timestamp, nonce, payload_bytes);
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts a key of any length");
    mac.update(canonical.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verifies `sig_hex` against the given fields in constant time.
pub fn verify(
    secret: &[u8],
    kind: &str,
    timestamp: i64,
    nonce: &str,
    payload_bytes: &[u8],
    sig_hex: &str,
) -> bool {
    let expected = sign(secret, kind, timestamp, nonce, payload_bytes);
    expected.as_bytes().ct_eq(sig_hex.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_round_trip() {
        let secret = b"node-secret-token";
        let sig = sign(
            secret,
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{\"ok\":true}",
        );
        assert!(verify(
            secret,
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{\"ok\":true}",
            &sig
        ));
    }

    #[test]
    fn verify_rejects_tampered_payload() {
        let secret = b"node-secret-token";
        let sig = sign(
            secret,
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{\"ok\":true}",
        );
        assert!(!verify(
            secret,
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{\"ok\":false}",
            &sig
        ));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let sig = sign(b"secret-a", "heartbeat", 1_700_000_000, "abc123", b"{}");
        assert!(!verify(
            b"secret-b",
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{}",
            &sig
        ));
    }

    #[test]
    fn verify_rejects_replayed_nonce_with_different_type() {
        // Same nonce/timestamp/payload but a different claimed type must not
        // validate against a signature computed for the original type —
        // otherwise an attacker could replay a captured message under a
        // different envelope type.
        let secret = b"node-secret-token";
        let sig = sign(secret, "event", 1_700_000_000, "abc123", b"{}");
        assert!(!verify(
            secret,
            "heartbeat",
            1_700_000_000,
            "abc123",
            b"{}",
            &sig
        ));
    }
}
