<div align="center">
  <h1>Vireon</h1>

  <p>
    <strong>A high-performance realtime runtime built on QUIC and io_uring for multi-stream, full-duplex communication at massive scale.</strong>
  </p>

  <p>
    <a href="https://github.com/vireon-duplex/Vireon/actions"><img src="https://img.shields.io/github/actions/workflow/status/vireon-duplex/Vireon/ci.yml?style=flat-square" alt="Build Status" /></a>
    <a href="https://github.com/vireon-duplex/Vireon/releases"><img src="https://img.shields.io/github/v/release/vireon-duplex/Vireon?style=flat-square" alt="Release" /></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" /></a>
    <a href="https://rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.90+-orange.svg?style=flat-square" alt="Rust Version" /></a>
  </p>
</div>

---

## 📖 About The Project

Vireon is a next-generation realtime runtime engineered for massive scale. Built entirely in **Rust** and leveraging the power of **QUIC** alongside **io_uring**, it provides an ultra-low latency, full-duplex communication layer capable of multiplexing millions of concurrent streams without breaking a sweat.

### ✨ Key Features

- **Blazing Fast I/O:** Utilizes `io_uring` for true asynchronous file and network operations, bypassing traditional kernel bottlenecks.
- **Next-Gen Transport:** Powered by QUIC for multiplexed, 0-RTT, full-duplex communication over UDP.
- **Clean Architecture:** Domain-centric design with pluggable ports for underlying storage and transport engines.
- **Highly Scalable:** Asynchronous at its core built on Tokio, operating exclusively with non-blocking, bounded channels.
- **Cloud-Native Ready:** Designed for containerized deployments with out-of-the-box observability (Prometheus metrics, OpenTelemetry tracing).

## 🏗 Architecture

Vireon employs a strict Clean Architecture strategy, isolating the core domain logic from infrastructure details. Transport and storage mechanisms are treated as interchangeable ports to allow for deep deployment flexibility.

<div align="center">
  <img src="resources/Vireon.png" alt="Vireon Architecture Diagram" />
</div>

For a deep dive into the design decisions and broker domain model, please consult our [Architecture Document](./ARCHITECTURE.md).

## 🚀 Getting Started

To get a local development instance of Vireon running, follow these steps.

### Prerequisites

Ensure you have the latest stable version of Rust installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Specific required components:
rustup component add rustfmt clippy
```

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/vireon-duplex/Vireon.git
   cd Vireon
   ```
2. Run the project in release mode for optimal performance
   ```bash
   cargo run --release
   ```

## 🧪 Testing & Quality Assurance

Vireon enforces strict code quality gates to maintain performance and safety. Before contributing or opening a PR, ensure all checks pass:

```bash
# Format and linting
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings

# Tests
cargo test --workspace

# Security and dependency audit
cargo audit
cargo deny check
```

For comprehensive details on our specific testing coverage and layers, refer to [TESTING.md](./TESTING.md).

## 🤝 Contributing

Contributions are what make the open-source community an amazing place to learn, inspire, and build robust software. Any contributions to Vireon are **greatly appreciated**.

Please review our [Contributing Guidelines](./CONTRIBUTING.md) and [Engineering Standards](./SKILLS.md) to understand our workflow and expectations before submitting your PR.

## 🛡 Security

If you discover a security vulnerability within Vireon, please do **NOT** open a public issue. Review our [Security Policy](./SECURITY.md) for the correct reporting procedures.

## 📄 License

Distributed under the MIT License. See [`LICENSE`](./LICENSE) for more information.
