import asyncio
import uuid
from unittest.mock import AsyncMock, patch

async def run_test():
    ats_source_id = uuid.uuid4()
    
    mock_db = AsyncMock()
    mock_db.execute.return_value.fetchone.return_value = ({"instance": "wd5", "career_site_name": "Careers"},)
    
    mock_session_factory = AsyncMock()
    mock_session_factory.return_value.__aenter__.return_value = mock_db
    
    from app.crawler.ats.workday import WorkdayCrawler
    crawler = WorkdayCrawler()
    
    page1 = {"jobPostings": [], "total": 0}
    with patch("app.db.AsyncSessionFactory", mock_session_factory):
        with patch.object(crawler, "_post_json", new=AsyncMock(return_value=page1)):
            jobs = await crawler.crawl("acme", ats_source_id)
            print(f"Jobs: {jobs}")

if __name__ == "__main__":
    asyncio.run(run_test())
