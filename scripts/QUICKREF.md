# Scripts Quick Reference

## Quick Commands

```bash
# Test environment
python scripts/test_scripts.py

# Incremental update (REST API)
python scripts/update_api.py

# Bulk download (S3 Bulk)
python scripts/update_bulk.py

# Data cleanup (dry-run mode)
python scripts/cleanup_fake_data.py
```

## File Map

| File | Function | Usage |
|------|----------|-------|
| `update_api.py` | REST API updater | Daily incremental updates |
| `update_bulk.py` | Bulk downloader | Initial bootstrap / large history |
| `cleanup_fake_data.py` | Data cleanup | Remove suspicious tail rows for delisted symbols |
| `test_scripts.py` | Test script | Validate environment setup |
| `README.md` | Full documentation | Detailed usage guide |
| `MIGRATION_SUMMARY.md` | Migration summary | Migration details |

## Common Config Changes

### Change market type
```python
# Find and modify in script
get_trading_pairs(
    market_type="swap"  # "spot" or "swap"
)
```

### Change interval
```python
update_params.update({
    "interval_list": ["1h"]  # ["1m", "5m", "1h", "4h", "1d"]
})
```

### Change concurrency
```python
update_params.update({
    "max_workers": 16  # worker thread count
})
```

## Recommended Workflow

```
┌─────────────────┐
│  First Run      │
│  test_scripts   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Initial Build  │
│  update_bulk    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Daily Update   │
│  update_api     │ ◄─── scheduled task
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Periodic Clean │
│  cleanup_fake   │ ◄─── monthly
└─────────────────┘
```

## Efficiency Comparison

| Operation | REST API | Bulk Download |
|----------|----------|---------------|
| 1 day data | Fast | Fast |
| 1 month data | Medium | Very fast |
| 1 year data | Slow | Very fast |

## Strategy Tips

- Initial bootstrap: use `update_bulk.py`
- Daily updates: use `update_api.py`
- Backfill missing data: use `update_bulk.py`
- Periodic cleanup: use `cleanup_fake_data.py` (dry-run first)

## Notes

- `cleanup_fake_data.py` uses dry-run by default and does not delete data.
- Always back up your database before real cleanup.
- Proxy config lives in `CryptoDB_feather/config.py`.
- Run `test_scripts.py` before first use.

## Quick Troubleshooting

```bash
# Problem: import failure
python scripts/test_scripts.py  # inspect detailed error output

# Problem: failed to fetch trading pairs
# Check PROXY in CryptoDB_feather/config.py

# Problem: invalid database path
# Check DB_ROOT_PATH in CryptoDB_feather/config.py
```

## Help

- See `scripts/README.md` for full docs
- See `scripts/MIGRATION_SUMMARY.md` for migration notes
- Run `python scripts/test_scripts.py` for environment diagnostics

---

Quick Reference | Version: 1.0.0 | Updated: 2026-01-14
