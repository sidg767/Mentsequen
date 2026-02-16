# Mentsequen

Mentsequen is a lightweight, high-performance blockchain sequencer built with Rust and the Axum web framework. It handles transaction ingestion, mempool management, block production, data availability (DA) persistence, and cryptographic verification.

## 🚀 Features

- **Transaction API**: Robust endpoint for submitting asynchronous transactions.
- **Mempool Management**: Thread-safe in-memory mempool for efficient transaction buffering.
- **Block Production**: Blocks include Merkle roots for transaction integrity and SHA-256 hashing for chain linking.
- **Data Availability (DA)**: Pluggable storage layer to persist blocks as JSON artifacts.
- **Cryptographic Verification**: Built-in ED25519 signature verification for secure transaction processing.
- **Real-time Updates**: WebSocket support for streaming new transactions to clients.

## 🛠 Tech Stack

- **Language**: Rust (Edition 2024)
- **Web Framework**: [Axum](https://github.com/tokio-rs/axum)
- **Runtime**: [Tokio](https://tokio.rs/)
- **Serialization**: [Serde](https://serde.rs/)
- **Hashing**: Sha2
- **Criptography**: Ed25519-dalek

## 📖 API Reference

### REST Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/tx` | Submit a new transaction. Body: `{"data": "..."}` |
| `GET` | `/mempool` | List all transactions currently in the mempool. |
| `GET` | `/block/:height` | Fetch a specific block by its height. |
| `GET` | `/head` | Get the latest block in the chain. |

### WebSockets

- `GET` `/ws`: Connect to the WebSocket endpoint to receive real-time transaction updates.

## 🚦 Getting Started
# Mentsequen

Mentsequen is a lightweight, high-performance blockchain sequencer written in Rust. It provides transaction ingestion, mempool management, block production, data availability (DA) persistence, and signature verification — suitable as a research or prototype sequencer.

**Quick links**
- **Source:** [src/](src)
- **Main binary:** [src/main.rs](src/main.rs)

**Highlights**
- Transaction API for submitting transactions.
- Thread-safe in-memory mempool for efficient buffering.
- Block production with Merkle roots and SHA-256 linking.
- Pluggable DA layer to persist blocks as JSON artifacts.
- ED25519 signature verification for transaction authenticity.
- Optional WebSocket feed for real-time transaction updates.

**Important files**
- [src/main.rs](src/main.rs): Application entrypoint and HTTP server setup.
- [src/lib.rs](src/lib.rs): Core library glue.
- [src/tx.rs](src/tx.rs): Transaction types and helpers.
- [src/mempool.rs](src/mempool.rs): In-memory mempool management.
- [src/block.rs](src/block.rs): Block structure and Merkle root logic.
- [src/da.rs](src/da.rs): Data-availability / persistence layer.
- [src/verify.rs](src/verify.rs): Signature and verification utilities.

**Tech stack**
- Rust (stable)
- Axum (HTTP server)
- Tokio (async runtime)
- Serde (serialization)

**Getting started**

Prerequisites: Rust toolchain (use `rustup`).

Clone and run:

```bash
git clone https://github.com/your-username/mentsequen.git
cd mentsequen
cargo run
```

The server defaults to `http://127.0.0.1:8080` (check [src/main.rs](src/main.rs) for port configuration).

Example: submit a transaction (JSON body depends on `src/tx.rs` types):

```bash
curl -X POST http://127.0.0.1:8080/tx -H 'Content-Type: application/json' \\
	-d '{"data":"...","sig":"...","pubkey":"..."}'
```

**Development**

Run tests and linters:

```bash
cargo test
cargo fmt --all
cargo clippy --all -- -D warnings
```

If you change interfaces or message formats, update integration tests in `tests/`.

**Contributing**
Contributions are welcome. Open an issue first to discuss larger changes.

**License**
MIT — see the LICENSE file.
