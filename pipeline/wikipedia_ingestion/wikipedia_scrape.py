import re
import time
import logging
import requests
from pathlib import Path

PIPELINE_DIR    = Path(__file__).parent
WIKI_USER_AGENT = "FinBot/1.0 (wikipedia-business-pipeline; contact@example.com)"
MAX_PER_CATEGORY = 2000

BUSINESS_CATEGORIES = [
    "Category:Business",
    "Category:Finance",
    "Category:Economics",
    "Category:Macroeconomics",
    "Category:Microeconomics",
    "Category:Monetary economics",
    "Category:Financial economics",
    "Category:Stock markets",
    "Category:Investment",
    "Category:Banking",
    "Category:Financial markets",
    "Category:Derivatives (finance)",
    "Category:Private equity",
    "Category:Venture capital",
    "Category:Companies",
    "Category:Management",
    "Category:Corporate finance",
    "Category:Mergers and acquisitions",
    "Category:Entrepreneurship",
    "Category:Marketing",
    "Category:Accounting",
    "Category:International trade",
    "Category:Financial regulation",
    "Category:Monetary policy",
    "Category:Fiscal policy",
]

BUSINESS_KEYWORDS = {
    "revenue", "profit", "earnings", "ebitda", "cash flow", "balance sheet",
    "market cap", "valuation", "ipo", "merger", "acquisition", "dividend",
    "stock", "equity", "bond", "yield", "gdp", "inflation", "interest rate",
    "federal reserve", "central bank", "monetary policy", "fiscal policy",
    "trade deficit", "supply chain", "venture capital", "private equity",
    "hedge fund", "bankruptcy", "restructuring", "layoffs", "regulation",
    "antitrust", "corporation", "startup", "entrepreneur", "shareholder",
    "accounting", "investment", "portfolio", "commodity", "real estate",
    "manufacturing", "retail", "banking", "insurance", "fintech",
    "cryptocurrency", "derivatives", "futures", "options trading",
    "economic growth", "recession", "unemployment", "consumer spending",
    "trade war", "tariff", "subsidy", "imf", "world bank", "wto", "oecd",
}


def get_category_members(category: str, max_articles: int = MAX_PER_CATEGORY) -> set:
    titles  = set()
    url     = "https://en.wikipedia.org/w/api.php"
    headers = {"User-Agent": WIKI_USER_AGENT}
    params  = {
        "action":  "query",
        "list":    "categorymembers",
        "cmtitle": category,
        "cmlimit": 500,
        "cmtype":  "page",
        "format":  "json",
    }

    retries     = 0
    max_retries = 3

    while len(titles) < max_articles:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15).json()
            retries  = 0
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                logging.warning(f"  API error for {category}: {e} — giving up after {max_retries} retries")
                break
            logging.warning(f"  API error for {category}: {e} — retry {retries}/{max_retries}")
            time.sleep(10 * (2 ** retries))
            continue

        for member in response.get("query", {}).get("categorymembers", []):
            titles.add(member["title"])

        if "continue" not in response:
            break
        params["cmcontinue"] = response["continue"]["cmcontinue"]
        time.sleep(0.3)

    return titles


def collect_business_titles() -> set:
    logging.info("=" * 55)
    logging.info("STEP 1: Collecting business article titles from Wikipedia categories")
    logging.info("=" * 55)

    all_titles = set()
    for i, category in enumerate(BUSINESS_CATEGORIES, 1):
        titles = get_category_members(category)
        logging.info(f"  [{i:02d}/{len(BUSINESS_CATEGORIES)}] {category}: {len(titles):,} articles")
        all_titles.update(titles)
        time.sleep(1.0)

    logging.info(f"\n  Total unique titles collected: {len(all_titles):,}\n")
    return all_titles
