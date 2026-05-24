## 2024-05-24 - [MEDIUM] Fix stack trace exposure in job API error handling
**Vulnerability:** Exposed sensitive data in logs/print statements.
**Learning:** Raw `print()` and `traceback.print_exc()` leak sensitive stack traces and DB internal structures instead of utilizing secure standard Python logging.
**Prevention:** Always use `logger.error(..., exc_info=True)` for exceptions to securely log them without exposing internals.
