## 2024-05-19 - Removed Stack Trace Leakage
**Vulnerability:** API endpoint exception blocks exposed database internal stack traces and internal errors to the standard output and error stream, which could be collected by a log aggregation system, exposing sensitive DB structure details or queries.
**Learning:** Raw `traceback.print_exc()` and `print(f"{e}")` should never be used in a production API codebase when handling database exceptions.
**Prevention:** Use standard Python `logging` with `logger.error("Message", exc_info=True)` which appropriately serializes exception details for central log management platforms securely, without leaking directly in plaintext logs or outputs unnecessarily.
