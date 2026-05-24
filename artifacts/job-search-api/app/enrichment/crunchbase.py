"""CrunchbaseEnricher — stub for future Crunchbase Basic API integration.

TODO: Crunchbase Basic API — https://www.crunchbase.com/api
Free tier: ~200 calls/month. Implement when API key is available.
Alternatives: Apollo.io free tier, People Data Labs free tier
"""


class CrunchbaseResult:
    def __init__(self):
        self.funding_total_usd: int | None = None
        self.last_funding_type: str | None = None
        self.sources: list[str] = []


class CrunchbaseEnricher:
    async def enrich(self, slug: str) -> CrunchbaseResult:
        raise NotImplementedError("CrunchbaseEnricher not yet implemented")
