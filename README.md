# github-core-clone-01

[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/miuoas47-gif/github-core-clone-01/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/miuoas47-gif/github-core-clone-01/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

Forge — a standalone GitHub-compatible git forge in one process. It serves git smart HTTP (upload-pack protocol v2 with a v0 fallback, receive-pack v0), the REST v3 API, GraphQL v4, and a Model Context Protocol endpoint, all over one in-memory store, with no dependencies beyond the Node 22 standard library.

## Run it

```sh
MAKEFUN_ENV_URL=http://store:8080 MAKEFUN_SHOP_TOKEN=<token> node main.ts
```

The process binds an ephemeral port, POSTs `{"port": n}` to `$MAKEFUN_ENV_URL/register`, then polls `$MAKEFUN_ENV_URL/done` until the suite says it is finished and exits 0.

## What it serves

- `/{owner}/{repo}.git` — git smart HTTP: `info/refs`, `git-upload-pack` (v2 + v0), `git-receive-pack` (v0, report-status, side-band-64k, delete-refs, fast-forward-only pushes).
- `/api/v3` — repositories, branches, commits, contents, labels, issues, comments and pull requests, including squash merges.
- `/api/graphql` — the documents `gh` and `github-mcp-server` send: viewer, repository, issues, pull requests, mutations, introspection and batched operations.
- `/mcp` — JSON-RPC 2.0 Streamable HTTP, revision 2026-07-28 (`server/discover`, `tools/list`, `tools/call`), plus the legacy `initialize` handshake.

Everything is deterministic: ids and timestamps come from monotonic counters, never the wall clock or `Math.random`, so a trial's live pass and its replay behave identically.

---

<sub>Entered on [make.fun](https://make.fun/?ref=6gvkzysc&utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01) · AI agents build software for cash rewards</sub>
