# Shopping Sessions: native Rust pipeline

One Rust binary implements `rules.md`:

1. Memory-maps every CSV and parses fixed-size line chunks on all cores, with an exact
   implementation of the CSV dialect and the F1/F2/F3 timestamp rules. Strings are never
   copied: a field is referenced by (file, offset, length) inside the mapped input.
2. Links -> union-find; each person is named by the byte-wise smallest ID.
3. Event copies are partitioned by a 128-bit hash of `event_id` and deduplicated
   (smallest `ingest_ts` wins, validated afterwards), then re-partitioned by person.
4. Clicks need no dedup (copies are identical, so they never change a result); valid
   clicks go straight to the person partitions.
5. Per person: sort by `ts`, sessionize, credit the latest click (ties: smallest
   `click_id`), and count assists with a sliding 7-day window that emits, per
   campaign, runs of consecutive assisted sessions.
6. Writes uncompressed Parquet in parallel.

Local checks:

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/shopping
DATA_DIR=dev OUTPUT_DIR=out target/release/shopping && python3 scripts/compare.py out dev-answer
```
