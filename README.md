# kotobase-server

Platform-independent XRPC handler + async/sync storage bridge for a
[`kotobase-peer`](https://github.com/kotoba-lang/kotobase-peer)-backed
worker. **No deploy surface** — this repo has no `wrangler.jsonc`, no
Cloudflare bindings, no storage adapter of its own. It's the shared
implementation a deployable Worker (e.g. `gftdcojp/net-kotobase`) wraps
with a thin, platform-specific shell.

## Why this exists

Two independent Cloudflare Worker deployments (`net-kotobase/kotobase-cf-wasm`
and `kotoba-lang/kotobase-cljc-worker`) each reimplemented the same
`ai.gftd.apps.kotobase.datomic.{datoms,transact,q,pull,fold}` XRPC surface
over `kotobase-peer`, independently, without either knowing about the
other. `kotobase-cljc-worker`'s version was further along (filtered reads,
retraction support, a lazy block-miss trampoline instead of a full-prefix
warm, real production incident fixes) — this repo extracts its
storage/platform-independent parts so there is ONE implementation, not two
drifting copies.

## What's here

```
kotobase.server.handler      -- pure XRPC dispatch, a function of an
                                 injected `store` map (get-fn/put!/head-get/
                                 head-put!/blind-fn/encrypt-fn/decrypt-fn).
                                 21 methods total:
                                   -- original 5: datoms/transact/q/pull/fold
                                   -- Datomic Cloud-style read helpers added
                                      since: entity/entid/ident/asOf/since/
                                      history/basisT/dbStats/seekDatoms/
                                      indexRange/indexPull/pullMany/tx/
                                      txRange/log/sync
kotobase.server.trampoline    -- with-blocks: bridges any async block store
                                 (R2, B2, IndexedDB, ...) to kotobase-peer's
                                 synchronous get-fn contract, fetching only
                                 the blocks a read/write actually touches
kotobase.server.ipns          -- pure decision logic for
                                 com.etzhayyim.apps.kotoba.ipns.{head,publish}
                                 (signature-gated writes, no CACAO)
kotobase.server.runtime       -- platform-neutral block/head/claim/authority/
                                 KMS/audit ports and fail-closed service
                                 validation for secured network runtimes
```

Storage adapters (R2/B2/IndexedDB), the actual HTTP/`Response` shell, routes,
and product policy are the consumer's job. CACAO/delegation verification,
normalized `SecurityContext`, keyring envelopes, audit receipts, replay and
idempotency orchestration are common security semantics and are being
consolidated here; deployable consumers must not fork them. The sole public
Cloudflare product owner is `gftdcojp/net-kotobase`.

## Crypto is injected, not policy

`kotobase-peer`'s `blind-fn`/`encrypt-fn`/`decrypt-fn` seam (ADR-2607051000,
no silent default) is threaded through `store` like the storage fns are —
the engine remains adapter-neutral. Private/sealed network runtimes use the
common versioned AES-256-GCM-SIV/keyring profile; plaintext passthrough is only
for explicit local/legacy-public use.

## Testing

```
npm install
npm run test:cljs
```
