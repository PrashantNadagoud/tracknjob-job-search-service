## 2024-05-06 - [Logging Database Errors Securely]
**Vulnerability:** Leaking stack traces to stdout and error details in error messages in `jobs.py`. `import traceback` and `traceback.print_exc()` is used in `try...except SQLAlchemyError` blocks.
**Learning:** Database exceptions might contain sensitive information about database schema and states.
**Prevention:** Use standard python `logging` module to log the exception and stack trace securely on the server side instead of using `traceback.print_exc()` and `print()`. Raise generic error messages for end users.
