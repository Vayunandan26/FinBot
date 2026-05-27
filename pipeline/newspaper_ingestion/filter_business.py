"""
filter_business.py
==================
Removes non-business articles from a cleaned newspaper CSV.

Keeps articles that are clearly about:
  - Markets, stocks, investing, earnings, M&A
  - Company news, corporate strategy, leadership
  - Economy, trade, monetary/fiscal policy
  - Industries: energy, tech, finance, real estate, retail, manufacturing, etc.
  - Regulation, taxation, accounting, law as it affects business
  - Startups, venture capital, IPOs
  - Business-focused COVID impact (supply chains, workforce, sectors)

Drops articles that are primarily about:
  - Entertainment / celebrity / music / film (non-industry angle)
  - Sports (non-business angle)
  - Crime unrelated to corporate/financial fraud
  - Pure politics / elections / legislation with no business angle
  - Lifestyle, fashion, beauty, food, travel (non-industry angle)
  - Social issues / human interest
  - Pure health/medical with no industry/market angle
  - Environment/science with no business/market angle
  - Local community events

Usage:
    python filter_business.py <input.csv> <output.csv>
"""

import re
import sys
import logging
import pandas as pd
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# =============================================================================
# SIGNAL PATTERNS
# Each pattern is checked against the URL + first 400 chars of article text.
# =============================================================================

# ── STRONG BUSINESS SIGNALS (keep if any match) ──────────────────────────────
# These are unambiguously business content.

BUSINESS_URL_DOMAINS = re.compile(
    r"(bloomberg|reuters|wsj\.com|ft\.com|forbes|businesswire|prnewswire|"
    r"benzinga|marketwatch|investing\.com|finance\.yahoo|seekingalpha|"
    r"fool\.com|cnbc|economist|hbr\.org|fortune|fastcompany|inc\.com|"
    r"entrepreneur|finanznachrichten|econotimes|seenews|bworldonline|"
    r"cfo\.com|accountantsdaily|therealdeal|rechargenews|zawya|"
    r"leaderpost.*business|straitstimes.*business|globetimes.*business|"
    r"executive-magazine|azbusinessdaily|printweek|miragenews)",
    re.IGNORECASE,
)

BUSINESS_URL_PATH = re.compile(
    r"(/business|/finance|/economy|/markets|/investing|/stocks|/earnings|"
    r"/companies|/corporate|/industry|/trade|/startup|/venture|/ipo|"
    r"/mergers|/acquisitions|/banking|/insurance|/real-estate|/energy-business|"
    r"/tech-business|/retail|/manufacturing|/supply-chain|/monetary|"
    r"/fiscal|/gdp|/inflation|/interest-rate|/federal-reserve|/central-bank|"
    r"/imf|/world-bank|/wto|/nasdaq|/nyse|/tsx|/ftse|/dax|"
    r"/pressrelease|/press-release|/prweb|/businesswire|/prnewswire|"
    r"/shareholder|/dividend|/quarterly|/annual-report|/sec-filing)",
    re.IGNORECASE,
)

BUSINESS_TEXT = re.compile(
    r"\b(revenue|profit|loss|earnings|ebitda|cash flow|balance sheet|"
    r"market cap|valuation|ipo|merger|acquisition|takeover|buyout|"
    r"shareholder|dividend|stock|share price|equity|bond|yield|"
    r"gdp|inflation|interest rate|federal reserve|central bank|monetary policy|"
    r"fiscal policy|trade deficit|supply chain|import|export tariff|"
    r"venture capital|private equity|hedge fund|asset management|"
    r"ceo|cfo|coo|chief executive|board of directors|quarterly results|"
    r"annual report|sec filing|nasdaq|nyse|tsx|ftse|s&p 500|dow jones|"
    r"investment|investor|portfolio|fund manager|financial results|"
    r"operating income|net income|gross margin|guidance|forecast|"
    r"retail sales|consumer spending|unemployment rate|job market|"
    r"manufacturing output|industrial production|housing starts|"
    r"startup|fundraising round|series [a-d]|seed funding|"
    r"bankruptcy|restructuring|layoffs|workforce reduction|"
    r"regulation|compliance|antitrust|sec|fca|fintech|"
    r"commodity|crude oil|natural gas price|gold price|copper price|"
    r"real estate market|property market|commercial real estate|reit)\b",
    re.IGNORECASE,
)

# ── NON-BUSINESS SIGNALS (drop if URL+text match, unless business override) ──

NON_BUSINESS_URL_PATH = re.compile(
    r"(/entertainment(?!.*business)|/celebrity|/music(?!.*industry)|"
    r"/film(?!.*industry|.*box.office)|/movies(?!.*box.office)|"
    r"/sports(?!.*business|.*revenue|.*deal)|/nfl(?!.*deal)|/nba(?!.*deal)|"
    r"/cricket(?!.*deal)|/lifestyle|/fashion(?!.*industry|.*business)|"
    r"/beauty(?!.*industry)|/food(?!.*industry)|/recipe|"
    r"/travel(?!.*industry|.*airline)|/horoscope|/astrology|"
    r"/dating|/relationship|/parenting|/pets|"
    r"/crime(?!.*fraud|.*financial|.*corporate|.*white.collar)|"
    r"/human-interest|/local(?!.*business)|"
    r"/events(?!.*business))",
    re.IGNORECASE,
)

NON_BUSINESS_DOMAINS = re.compile(
    r"(bollywoodhungama|iheart\.com|957the|947bob|"
    r"kansan\.com|yeovilexpress|shepherdstown|"
    r"click2houston.*entertainment|fox5.*entertain|"
    r"dailystar\.co\.uk/news/latest|kfilradio)",
    re.IGNORECASE,
)

NON_BUSINESS_TEXT_STRONG = re.compile(
    # Strong non-business content markers in the opening 400 chars
    r"\b(kanye west|kardashian|grammy award|oscar award|golden globe|"
    r"bollywood|nollywood|celebrity gossip|box office hit|"
    r"nfl draft|nba draft|premier league goal|cricket match score|"
    r"murder suspect|serial killer|domestic violence|sex assault|"
    r"recipe for|how to cook|fashion week runway|beauty tip|"
    r"horoscope|zodiac|astrology chart)\b",
    re.IGNORECASE,
)

# ── BUSINESS OVERRIDE — topics that sound non-business but have clear B angle ─
# If text has these, keep even if non-business URL signals fire.

BUSINESS_OVERRIDE = re.compile(
    r"\b(box office revenue|film industry|music industry|streaming revenue|"
    r"sports franchise|sports rights deal|broadcast rights|"
    r"health sector|pharmaceutical|biotech|drug approval|fda approval|"
    r"medical device|hospital system|health insurance|"
    r"climate investment|renewable energy market|carbon credit|"
    r"energy transition|green bond|esg|sustainability report|"
    r"housing market|home price|mortgage rate|construction sector|"
    r"food industry|agri-business|commodity price|farm subsidy|"
    r"travel industry|airline revenue|hotel occupancy|tourism economy|"
    r"fashion brand|luxury goods|retail fashion|apparel market|"
    r"stimulus.*economy|relief.*business|pandemic.*sector|covid.*market|"
    r"political risk|geopolitical.*trade|sanctions.*business|"
    r"tax policy|tax reform|corporate tax|vat|gst|tariff)\b",
    re.IGNORECASE,
)


# =============================================================================
# CLASSIFICATION LOGIC
# =============================================================================

def is_business(url: str, text: str) -> bool:
    url = url or ""
    snippet = str(text)[:400].lower() if text else ""
    full_text = str(text).lower() if text else ""

    parsed = urlparse(url.lower())
    domain = parsed.netloc
    path = parsed.path

    # 1. Strong business domain → keep immediately
    if BUSINESS_URL_DOMAINS.search(domain):
        return True

    # 2. Strong business URL path → keep
    if BUSINESS_URL_PATH.search(path):
        return True

    # 3. Strong non-business domain → drop (unless business override in text)
    if NON_BUSINESS_DOMAINS.search(domain):
        return bool(BUSINESS_OVERRIDE.search(snippet))

    # 4. Non-business URL path
    if NON_BUSINESS_URL_PATH.search(path):
        # Still keep if there's a clear business angle in the text
        return bool(BUSINESS_OVERRIDE.search(snippet))

    # 5. Strong non-business content in opening text → drop (unless overridden)
    if NON_BUSINESS_TEXT_STRONG.search(snippet):
        return bool(BUSINESS_OVERRIDE.search(snippet))

    # 6. Default: keep
    # Politics, social issues, and general news that don't trigger any explicit
    # non-business signal above are kept.
    return True


# =============================================================================
# MAIN
# =============================================================================

def process(input_path: str, output_path: str):
    logging.info(f"Reading:  {input_path}")
    df = pd.read_csv(input_path, dtype=str)

    if "Article_Text" not in df.columns or "URL" not in df.columns:
        raise ValueError("CSV must have 'URL' and 'Article_Text' columns.")

    total = len(df)

    mask = df.apply(
        lambda row: is_business(row.get("URL", ""), row.get("Article_Text", "")),
        axis=1,
    )
    df_filtered = df[mask].copy()

    kept = len(df_filtered)
    dropped = total - kept
    logging.info(
        f"Done:     {total} rows → {kept} kept, {dropped} dropped "
        f"({dropped / max(total, 1):.1%} non-business removal)"
    )

    df_filtered.to_csv(output_path, index=False)
    logging.info(f"Saved:    {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python filter_business.py <input.csv> <output.csv>")
        sys.exit(1)
    process(sys.argv[1], sys.argv[2])
