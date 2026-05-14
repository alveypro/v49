# Maintenance Tool Archive

This namespace contains one-off extraction or assembly helpers used during
previous refactors. They should not be part of daily operations.

If a maintenance helper is needed again, prefer replacing it with a deterministic
service-level build step and a regression test.

`assemble_top5_rebuild_service.py` is retained as historical migration context
only. Its former `_top5_rebuild_body*` intermediate inputs were generated
artifacts, not production service sources.
