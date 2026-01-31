# Contributing to Scry

Thank you for your interest in contributing to Scry! This document provides guidelines and information for contributors.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to opensource@scrydata.com.

## Contributor License Agreement (CLA)

Before we can accept your contributions, you must sign our Contributor License Agreement (CLA). This is a one-time requirement that protects both you and ScryData.

When you open your first pull request, the [CLA Assistant](https://cla-assistant.io/) will guide you through the signing process. The CLA ensures that:

- You have the right to submit the contribution
- ScryData can distribute your contribution under the project's license
- Your contribution remains available under open source terms

## How to Contribute

### Reporting Bugs

Before reporting a bug, please:

1. Search [existing issues](https://github.com/scrydata/scry-proxy/issues) to avoid duplicates
2. Check if the issue persists on the latest version

When reporting, include:

- Scry version (`scry --version`)
- Operating system and version
- Rust version (`rustc --version`)
- Steps to reproduce the issue
- Expected vs actual behavior
- Relevant logs or error messages

### Suggesting Features

We welcome feature suggestions! Please:

1. Search [existing issues](https://github.com/scrydata/scry-proxy/issues) first
2. Open a new issue with the "feature request" label
3. Describe the use case and expected behavior
4. Explain why this would benefit other users

### Submitting Pull Requests

1. **Fork the repository** and create your branch from `main`

2. **Set up your development environment**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/scry-proxy.git
   cd scry-proxy
   just build
   just test
   ```

3. **Make your changes**:
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

4. **Run the test suite**:
   ```bash
   just ci  # Runs fmt, lint, and all tests
   ```

5. **Commit your changes**:
   - Use clear, descriptive commit messages
   - Reference related issues (e.g., "Fixes #123")

6. **Push and open a pull request**:
   - Describe what changes you made and why
   - Link to related issues
   - Ensure CI passes

### Pull Request Guidelines

- Keep PRs focused on a single concern
- Include tests for bug fixes and new features
- Update documentation for user-facing changes
- Maintain backwards compatibility when possible
- Follow Rust idioms and best practices

## Development Setup

### Prerequisites

- Rust (latest stable)
- Docker (for integration tests)
- [just](https://github.com/casey/just) command runner

### Common Commands

```bash
just build              # Build the project
just test               # Run all tests
just test-unit          # Run unit tests only
just test-integration   # Run integration tests (requires Docker)
just lint               # Run clippy linter
just fmt                # Format code
just ci                 # Run all CI checks
```

### Running Locally

```bash
# Start a local Postgres for testing
just postgres-up

# Run the proxy
just run

# Connect through the proxy
psql -h 127.0.0.1 -p 5433 -U postgres
```

## Code Style

- Follow standard Rust formatting (`rustfmt`)
- Use `clippy` and address all warnings
- Write documentation for public APIs
- Prefer explicit types over inference when it aids readability
- Use meaningful variable and function names

## Testing

- Write unit tests for new functionality
- Add integration tests for end-to-end scenarios
- Ensure tests are deterministic and don't rely on timing
- Use `testcontainers` for tests requiring Postgres

## Documentation

- Update relevant documentation for user-facing changes
- Add inline documentation for complex logic
- Keep README.md and docs/ in sync

## Getting Help

- Open an issue for questions about contributing
- Join discussions on GitHub Issues
- Email opensource@scrydata.com for other inquiries

## Recognition

Contributors will be recognized in release notes. Thank you for helping make Scry better!

## License

By contributing to Scry, you agree that your contributions will be licensed under the same terms as the project: MIT OR Apache-2.0 (dual-licensed).
