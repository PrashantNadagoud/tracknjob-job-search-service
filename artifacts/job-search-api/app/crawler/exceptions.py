"""ATS crawler exception hierarchy.

All exceptions raised by ATS crawlers derive from `CrawlerError`.
The `CrawlDispatcher` catches these to apply the correct back-off strategy.
"""


class CrawlerError(Exception):
    """Base for all ATS crawler errors."""

    def __init__(self, message: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class RateLimitedException(CrawlerError):
    """HTTP 429 or equivalent throttle response from the ATS."""


class SlugNotFoundException(CrawlerError):
    """The company slug / ATS config was not found (HTTP 404).

    Triggers permanent deactivation of the AtsSource row.
    """


class CrawlException(CrawlerError):
    """Generic crawl failure: HTTP 5xx, JSON parse error, network timeout, etc."""
