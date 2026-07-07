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
kotobase.server.handler      -- pure XRPC dispatch (do-datoms/do-transact/
                                 do-q/do-pull/do-fold), a function of an
                                 injected `store` map (get-fn/put!/head-get/
                                 head-put!/blind-fn/encrypt-fn/decrypt-fn)
kotobase.server.trampoline    -- with-blocks: bridges any async block store
                                 (R2, B2, IndexedDB, ...) to kotobase-peer's
                                 synchronous get-fn contract, fetching only
                                 the blocks a read/write actually touches
kotobase.server.ipns          -- pure decision logic for
                                 com.etzhayyim.apps.kotoba.ipns.{head,publish}
                                 (signature-gated writes, no CACAO)
```

Storage (R2/B2/IndexedDB adapters), CAS-with-retry orchestration, CACAO
verification, and the actual HTTP/`Response` shell are all the CONSUMER's
job — see `kotoba-lang/kotobase-cljc-worker`'s `worker.cljc` (R2/Cloudflare)
or `kotoba-lang/kotobase-browser-worker`'s equivalent (IndexedDB) for
reference shells.

## Crypto is injected, not policy

`kotobase-peer`'s `blind-fn`/`encrypt-fn`/`decrypt-fn` seam (ADR-2607051000,
no silent default) is threaded through `store` like the storage fns are —
this repo doesn't ship a default profile. A consumer picks a
plaintext-passthrough profile (the pre-encryption interim posture several
current deployments use) or a real AEAD profile (HKDF-derived AES-256-GCM +
HMAC blind, for example) by supplying those three functions; nothing here
needs to change either way.

## Testing

```
npm install
npm run test:cljs
```
