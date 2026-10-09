# fuzz

Three libFuzzer targets, run weekly by `models.yml` and by hand with a nightly toolchain:

```
cargo install cargo-fuzz --locked
cd fuzz
python3 seeds.py unpack
cargo +nightly fuzz run format_parsers -- -max_total_time=1200 -rss_limit_mb=4096
```

## seeds

`seeds/<target>.tar.gz` is each target's starting corpus, and `corpus/` is where a run reads and
grows it. Only `seeds.py` makes the archives, never `tar`. Each one is a ustar stream of the units
in name order with mode 0644, uid and gid 0 and one fixed mtime, gzipped at level 9 with no name or
timestamp. Dotfiles are skipped, so a Mac's AppleDouble files stay out. The tar under the gzip is
byte for byte the same on macOS and Linux. The gzip wrapper can differ by zlib build, so `check`
compares the tar.

To grow a corpus, run a target for a while, shrink what it kept, pack, and commit:

```
cargo +nightly fuzz run format_parsers -- -max_total_time=3600
cargo +nightly fuzz cmin format_parsers
python3 seeds.py pack format_parsers
python3 seeds.py check format_parsers
```
