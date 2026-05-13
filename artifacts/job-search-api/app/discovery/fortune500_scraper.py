import logging
import re
import asyncio
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

FORTUNE500_FALLBACK = [
    {"company_name": "Walmart", "website": "walmart.com", "industry": "Retail"},
    {"company_name": "Amazon", "website": "amazon.com", "industry": "Technology"},
    {"company_name": "Apple", "website": "apple.com", "industry": "Technology"},
    {"company_name": "CVS Health", "website": "cvshealth.com", "industry": "Healthcare"},
    {"company_name": "UnitedHealth Group", "website": "unitedhealthgroup.com", "industry": "Healthcare"},
    {"company_name": "Exxon Mobil", "website": "exxonmobil.com", "industry": "Energy"},
    {"company_name": "Berkshire Hathaway", "website": "berkshirehathaway.com", "industry": "Finance"},
    {"company_name": "Alphabet", "website": "abc.xyz", "industry": "Technology"},
    {"company_name": "McKesson", "website": "mckesson.com", "industry": "Healthcare"},
    {"company_name": "Cigna", "website": "cigna.com", "industry": "Healthcare"},
    {"company_name": "AT&T", "website": "att.com", "industry": "Telecom"},
    {"company_name": "Microsoft", "website": "microsoft.com", "industry": "Technology"},
    {"company_name": "Costco", "website": "costco.com", "industry": "Retail"},
    {"company_name": "JPMorgan Chase", "website": "jpmorganchase.com", "industry": "Finance"},
    {"company_name": "Chevron", "website": "chevron.com", "industry": "Energy"},
    {"company_name": "Home Depot", "website": "homedepot.com", "industry": "Retail"},
    {"company_name": "Walgreens", "website": "walgreens.com", "industry": "Retail"},
    {"company_name": "Bank of America", "website": "bankofamerica.com", "industry": "Finance"},
    {"company_name": "Marathon Petroleum", "website": "marathonpetroleum.com", "industry": "Energy"},
    {"company_name": "Anthem", "website": "anthem.com", "industry": "Healthcare"},
    {"company_name": "Verizon", "website": "verizon.com", "industry": "Telecom"},
    {"company_name": "Ford Motor", "website": "ford.com", "industry": "Automotive"},
    {"company_name": "General Motors", "website": "gm.com", "industry": "Automotive"},
    {"company_name": "Centene", "website": "centene.com", "industry": "Healthcare"},
    {"company_name": "Meta Platforms", "website": "meta.com", "industry": "Technology"},
    {"company_name": "Comcast", "website": "comcast.com", "industry": "Media"},
    {"company_name": "Phillips 66", "website": "phillips66.com", "industry": "Energy"},
    {"company_name": "Valero Energy", "website": "valero.com", "industry": "Energy"},
    {"company_name": "Dell Technologies", "website": "dell.com", "industry": "Technology"},
    {"company_name": "Target", "website": "target.com", "industry": "Retail"},
    {"company_name": "Humana", "website": "humana.com", "industry": "Healthcare"},
    {"company_name": "FedEx", "website": "fedex.com", "industry": "Logistics"},
    {"company_name": "Fannie Mae", "website": "fanniemae.com", "industry": "Finance"},
    {"company_name": "Freddie Mac", "website": "freddiemac.com", "industry": "Finance"},
    {"company_name": "Goldman Sachs", "website": "goldmansachs.com", "industry": "Finance"},
    {"company_name": "Raytheon Technologies", "website": "rtx.com", "industry": "Defense"},
    {"company_name": "Boeing", "website": "boeing.com", "industry": "Defense"},
    {"company_name": "Lockheed Martin", "website": "lockheedmartin.com", "industry": "Defense"},
    {"company_name": "HP", "website": "hp.com", "industry": "Technology"},
    {"company_name": "UPS", "website": "ups.com", "industry": "Logistics"},
    {"company_name": "AbbVie", "website": "abbvie.com", "industry": "Pharma"},
    {"company_name": "Johnson & Johnson", "website": "jnj.com", "industry": "Healthcare"},
    {"company_name": "Pfizer", "website": "pfizer.com", "industry": "Pharma"},
    {"company_name": "Caterpillar", "website": "caterpillar.com", "industry": "Manufacturing"},
    {"company_name": "Deere & Company", "website": "deere.com", "industry": "Manufacturing"},
    {"company_name": "IBM", "website": "ibm.com", "industry": "Technology"},
    {"company_name": "Intel", "website": "intel.com", "industry": "Technology"},
    {"company_name": "Salesforce", "website": "salesforce.com", "industry": "Technology"},
    {"company_name": "Oracle", "website": "oracle.com", "industry": "Technology"},
    {"company_name": "Netflix", "website": "netflix.com", "industry": "Media"},
]

class Fortune500Scraper:
    def __init__(self):
        self.rank_limit = None

    def _derive_website(self, company_name: str) -> Optional[str]:
        # Check fallback list first
        for item in FORTUNE500_FALLBACK:
            if item["company_name"].lower() == company_name.lower():
                return item["website"]
        
        # Derivation logic: lowercase + strip punctuation + .com
        clean_name = re.sub(r'[^a-zA-Z0-9]', '', company_name.lower())
        
        # Ambiguous names check (very short or missing)
        if len(clean_name) < 2:
            return None
            
        return f"{clean_name}.com"

    async def _fetch_wikipedia(self) -> List[Dict[str, Any]]:
        urls_to_try = [
            "https://en.wikipedia.org/wiki/Fortune_500",
            "https://en.wikipedia.org/wiki/List_of_Fortune_500_companies"
        ]
        
        headers = {"User-Agent": "JobSearchBot/1.0 (research@example.com)"}
        
        for url in urls_to_try:
            try:
                async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    
                soup = BeautifulSoup(resp.text, 'html.parser')
                tables = soup.find_all('table', {'class': 'wikitable'})
                
                # Look for the table with headers containing Rank, Company, Industry
                target_table = None
                for table in tables:
                    rows = table.find_all('tr')
                    if not rows:
                        continue
                        
                    headers_text = [th.text.strip().lower() for th in rows[0].find_all(['th', 'td'])]
                    has_rank = any('rank' in h for h in headers_text)
                    has_company = any('name' in h or 'company' in h for h in headers_text)
                    has_industry = any('industry' in h for h in headers_text)
                    
                    if has_rank and has_company and has_industry:
                        target_table = table
                        break
                        
                if not target_table:
                    continue
                    
                rows = target_table.find_all('tr')
                if len(rows) < 100:
                    logger.debug(f"Table at {url} only has {len(rows)} rows, skipping to next URL...")
                    continue
                    
                headers_text = [th.text.strip().lower() for th in rows[0].find_all(['th', 'td'])]
                
                # We need: Rank, Company name, Industry, Revenue (USD millions), Employees, Headquarters
                rank_idx, company_idx, industry_idx, employees_idx, hq_idx = -1, -1, -1, -1, -1
                
                for i, header in enumerate(headers_text):
                    if 'rank' in header: rank_idx = i
                    elif 'name' in header or 'company' in header: company_idx = i
                    elif 'industry' in header: industry_idx = i
                    elif 'employees' in header: employees_idx = i
                    elif 'headquarters' in header: hq_idx = i
                    
                if company_idx == -1:
                    continue
                    
                companies = []
                for row in rows[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) <= company_idx:
                        continue
                        
                    company_name = cols[company_idx].text.strip()
                    if not company_name:
                        continue
                        
                    rank = None
                    if rank_idx != -1 and len(cols) > rank_idx:
                        try:
                            rank = int(re.sub(r'[^0-9]', '', cols[rank_idx].text.strip()))
                        except ValueError:
                            pass
                            
                    industry = None
                    if industry_idx != -1 and len(cols) > industry_idx:
                        industry = cols[industry_idx].text.strip()
                        
                    employees = None
                    if employees_idx != -1 and len(cols) > employees_idx:
                        try:
                            employees = int(re.sub(r'[^0-9]', '', cols[employees_idx].text.strip()))
                        except ValueError:
                            pass
                            
                    hq = None
                    if hq_idx != -1 and len(cols) > hq_idx:
                        hq = cols[hq_idx].text.strip()
                        
                    companies.append({
                        "company_name": company_name,
                        "website": self._derive_website(company_name),
                        "industry": industry,
                        "rank": rank,
                        "employees": employees,
                        "headquarters": hq,
                        "source": "fortune500"
                    })
                    
                if len(companies) >= 100:
                    logger.info(f"Found target table at {url} with {len(companies)} parsed companies.")
                    return companies
                    
            except Exception as e:
                logger.warning(f"Failed to fetch or parse {url}: {e}")
                continue
                
        raise ValueError("Could not find a valid wikitable with >= 100 Fortune 500 companies across tried URLs")

    async def _fetch_github(self) -> List[Dict[str, Any]]:
        url = "https://raw.githubusercontent.com/cFortune/fortune500/master/fortune500.json"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            
        data = resp.json()
        companies = []
        for item in data:
            company_name = item.get("companyName") or item.get("name")
            if not company_name:
                continue
            
            companies.append({
                "company_name": company_name,
                "website": self._derive_website(company_name),
                "industry": item.get("industry"),
                "rank": item.get("rank"),
                "employees": item.get("employees"),
                "headquarters": item.get("hq") or item.get("headquarters"),
                "source": "fortune500"
            })
            
        return companies

    def _get_fallback(self) -> List[Dict[str, Any]]:
        companies = []
        for i, item in enumerate(FORTUNE500_FALLBACK):
            companies.append({
                "company_name": item["company_name"],
                "website": item.get("website") or self._derive_website(item["company_name"]),
                "industry": item.get("industry"),
                "rank": i + 1,
                "employees": None,
                "headquarters": None,
                "source": "fortune500"
            })
        return companies

    async def fetch(self) -> List[Dict[str, Any]]:
        companies = []
        source_used = None
        
        try:
            companies = await self._fetch_wikipedia()
            source_used = "wikipedia"
        except Exception as e:
            logger.warning(f"Wikipedia fetch failed: {e}. Trying GitHub fallback.")
            try:
                companies = await self._fetch_github()
                source_used = "github"
            except Exception as e:
                logger.warning(f"GitHub fallback failed: {e}. Using hardcoded fallback.")
                companies = self._get_fallback()
                source_used = "hardcoded"
                
        if self.rank_limit is not None:
            # Filter and sort by rank
            companies = [c for c in companies if c.get("rank") is not None and c["rank"] <= self.rank_limit]
            
        logger.info(f"Fortune500Scraper used source '{source_used}' and returned {len(companies)} companies.")
        return companies
