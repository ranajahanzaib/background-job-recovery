# Contributing

## Development setup

Fork [the repository](https://github.com/ranajahanzaib/background-job-recovery), clone your fork, and create a topic branch:

```sh
git clone https://github.com/YOUR_USERNAME/background-job-recovery.git
cd background-job-recovery
cp .env.example .env
```

Replace the credential placeholders in `.env` with locally generated values before starting Compose. See the [README](README.md#local-setup) for the credential setup and development commands.

## Changes

- Keep changes focused and follow the existing Go formatting and SQL conventions.
- Add or update tests for behavior changes. Database or JetStream behavior should use the real services in the integration suite.
- Run `make test`, `make test-race`, `make lint`, and `make build` for Go changes. Run `make test-integration` for changes that affect PostgreSQL or NATS behavior.
- Update the relevant documentation when a change affects behavior, configuration, operations, or reliability claims.
- Do not include credentials, tokens, private data, or generated build artifacts in a contribution.

Open an issue for bugs or feature proposals. Pull requests should describe the problem, the behavior changed, and the validation performed.
