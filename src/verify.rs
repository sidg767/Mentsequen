use anyhow::{anyhow, Result};
use ed25519_dalek::{PublicKey, Signature, Verifier};
use base64::engine::general_purpose;
use base64::Engine;

pub fn verify_ed25519(pubkey_b64: &str, sig_b64: &str, message: &[u8]) -> Result<()> {
    let pk_bytes = general_purpose::STANDARD
        .decode(pubkey_b64)
        .map_err(|e| anyhow!("pubkey base64 decode: {}", e))?;
    let sig_bytes = general_purpose::STANDARD
        .decode(sig_b64)
        .map_err(|e| anyhow!("sig base64 decode: {}", e))?;

    let pk = PublicKey::from_bytes(&pk_bytes)
        .map_err(|e| anyhow!("invalid public key bytes: {}", e))?;
    let sig = Signature::from_bytes(&sig_bytes)
        .map_err(|e| anyhow!("invalid signature bytes: {}", e))?;

    pk.verify(message, &sig)
        .map_err(|e| anyhow!("signature verify failed: {}", e))
}

