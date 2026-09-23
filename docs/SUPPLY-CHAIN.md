# Supply chain: verifying release assets

Every release asset of the community build (binaries, pacman packages, PGO
variants) is attested with [GitHub artifact attestations](https://github.com/actions/attest-build-provenance)
at build time. The attestation is a signed provenance record binding the
asset's SHA-256 to the exact workflow run, commit, and repository that
produced it — no intermediary to trust between this repository and the
binary you install.

## Verify before installing

With the [GitHub CLI](https://cli.github.com/) (v2.49+):

```sh
# download an asset from the release page, then:
gh attestation verify ripgrep_all-v0.10.10.3-x86_64-unknown-linux-gnu.tar.gz \
  --repo danalec/ripgrep-all
```

or verify directly against a local package:

```sh
gh attestation verify ripgrep-all-znver4-0.10.10.3-1-x86_64.pkg.tar.zst \
  --repo danalec/ripgrep-all
```

A valid verification prints the subject digest, the workflow name
(`release`, `packaging`, or `pgo`), the commit SHA, and the Sigstore
transparency-log entry. Anything else means the file was not produced by
this repository's CI — do not install it.

## What is covered

| asset kind                  | workflow     |
|-----------------------------|--------------|
| release tarballs/zips       | `release`    |
| pacman `.pkg.tar.zst`       | `packaging`  |
| `-pgo` variants             | `pgo`        |

## Limits

- Attestation proves *provenance* (which repo/commit/CI run built the
  artifact), not that the code is bug-free.
- Older assets published before attestation was enabled carry no
  attestation record.
- pacman has no native Sigstore verification; the `gh attestation verify`
  step above is the check for now.
