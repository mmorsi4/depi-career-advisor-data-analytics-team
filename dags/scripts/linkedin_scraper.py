import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import urllib.parse
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata
from rapidfuzz import process, fuzz
import numpy as np
import json
from dotenv import load_dotenv
import os

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# =======================
# Constants & Configuration
# =======================
APP_ID = os.getenv("APP_ID")
APP_KEY = os.getenv("APP_KEY")
COUNTRY = "us"
ONET_CSV_PATH = os.path.join(BASE_DIR, "data", "onet_job_titles.csv")
MATCH_THRESHOLD = 0  # Minimum similarity score to accept
PPP_ADJUSTMENT_FACTOR = 0.7
WORLD_BANK_PPP_URL = "https://api.worldbank.org/v2/country/EG/indicator/PA.NUS.PPP?format=json"
ADZUNA_HISTOGRAM_URL = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/histogram"
SLEEP_INTERVAL = 1  # seconds between requests

# =======================
# Load Data
# =======================
onet_df = pd.read_csv(ONET_CSV_PATH)
onet_titles = onet_df['Title'].dropna().str.lower().tolist()

# =======================
# Helper Functions
# =======================
def map_to_onet(title):
    title_lower = title.lower()
    match, score, _ = process.extractOne(title_lower, onet_titles, scorer=fuzz.token_sort_ratio)
    if score >= MATCH_THRESHOLD:
        return match, score
    return None, None

def get_ppp():
    res = requests.get(WORLD_BANK_PPP_URL)
    data = res.json()
    records = data[1]
    records.sort(key=lambda x: int(x['date']), reverse=True)
    return records[0]['value'] * PPP_ADJUSTMENT_FACTOR

PPP = get_ppp()

def get_salary_histogram(job_title):
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": job_title,
    }
    response = requests.get(ADZUNA_HISTOGRAM_URL, params=params)
    data = response.json()
    return data.get("histogram", {})

def aggregate_stats(histogram):
    counter = 0
    salaries = []
    for salary_str, count in histogram.items():
        salary = int(salary_str)
        counter += count
        salaries.extend([salary] * count)
    if not salaries:
        return None, None, None, None
    mean_salary = np.mean(salaries)
    median_salary = np.median(salaries)
    percentiles = np.percentile(salaries, [10, 90])
    return mean_salary, median_salary, percentiles, counter

def clean_job_description(raw_html):
    """
    Cleans raw HTML text from a job description.
    - Removes HTML tags, preserving line breaks for lists.
    - Fixes common encoding errors (mojibake).
    - Normalizes whitespace.
    """
    if not raw_html:
        return ""

    # 1. Parse HTML and extract text
    # We use separator="\n" to add newlines where tags like <p> or <li> end.
    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # 2. Fix common encoding errors (mojibake)
    # This dictionary fixes symbols like the 'â€™' in your example
    mojibake_fixes = {
        'â€™': "'",
        'â€œ': '"',
        'â€': '"',
        'â€“': '–',
        'â€”': '—',
        'â€¢': '•',
        'â€¦': '…',
        'Â': '', 
        'â€‹': '',
    }
    
    for bad, good in mojibake_fixes.items():
        text = text.replace(bad, good)

    # 3. Normalize whitespace and characters
    # NFKC normalizes characters (e.g., turning fancy quotes into standard ones)
    text = unicodedata.normalize('NFKC', text)
    
    # Replace non-breaking spaces with regular spaces
    text = text.replace('\u00a0', ' ').replace('\xa0', ' ')
    
    # Consolidate multiple spaces into one
    text = re.sub(r'[ \t]+', ' ', text)
    
    # Consolidate multiple newlines (more than 2) into just two
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    
    return text.strip()




def clean_text(text):
    """Clean text by fixing encoding issues and normalizing characters"""
    if not text:
        return text

    mojibake_fixes = {
        'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€“': '–', 'â€”': '—',
        'â€¢': '•', 'â€¦': '…', 'Â': '', 'â€‹': '', 'â€Š': ' ',
        'Ã©': 'é', 'Ã¨': 'è', 'Ã¡': 'á', 'Ã ': 'à', 'Ã³': 'ó',
        'Ã²': 'ò', 'Ã­': 'í', 'Ã±': 'ñ', 'Ã§': 'ç', 'Ã¼': 'ü',
        'Ã¶': 'ö', 'Ã¤': 'ä',
    }
    for bad, good in mojibake_fixes.items():
        text = text.replace(bad, good)

    text = unicodedata.normalize('NFKC', text)
    text = text.replace('\u00a0', ' ').replace('\xa0', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def create_session():
    """Create a session with retry strategy and proper headers"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36"
    })
    return session


def get_absolute_url(href: str):
    """Return absolute LinkedIn URL or None."""
    if not href:
        return None
    if href.startswith("/"):
        return "https://www.linkedin.com" + href
    if href.startswith("http"):
        return href
    return None


def get_full_job_description(job_soup):
    """
    Extract the full job description (no filtering).
    Tries multiple methods to ensure maximum data capture.
    """
    # Try JSON-LD
    json_ld_scripts = job_soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                data = [data]
            for item in data:
                if item.get('@type') == 'JobPosting' and 'description' in item:
                    return clean_text(item['description'])
        except Exception:
            continue

    # Try main visible container
    desc_div = job_soup.find("div", class_=re.compile(r"(show-more-less-html__markup|description__text)"))
    if desc_div:
        return clean_text(desc_div.get_text("\n", strip=True))

    # Try fallback containers
    candidates = job_soup.find_all(["div", "section"], class_=re.compile(r"(description|job-description|details)"))
    for c in candidates:
        text = c.get_text("\n", strip=True)
        if len(text) > 100:
            return clean_text(text)

    # Fallback: entire main content
    main = job_soup.find("main") or job_soup.find("article")
    if main:
        return clean_text(main.get_text("\n", strip=True))

    return None


def is_valid_location(text):
    """Simple heuristic for valid location"""
    if not text:
        return False
    return bool(re.search(r'\b(remote|egypt|cairo|dubai|saudi|uk|usa|canada|germany|france|australia)\b', text.lower()))


def extract_location(job_soup, company_name=None):
    """Extract job location"""
    loc_elems = job_soup.find_all(['span', 'div'], class_=re.compile(r'topcard__flavor'))
    for elem in loc_elems:
        text = elem.get_text(strip=True)
        if is_valid_location(text):
            return text
    return None


def extract_employment_type(job_soup):
    """Extract employment type info"""
    patterns = r'\b(full.?time|part.?time|contract|internship|temporary|freelance)\b'
    all_text = job_soup.get_text().lower()
    match = re.search(patterns, all_text)
    return match.group(1).title() if match else None


def extract_job_flexibility(job_soup):
    """Extract remote/hybrid/on-site info"""
    all_text = job_soup.get_text().lower()
    if "remote" in all_text:
        return "Remote"
    if "hybrid" in all_text:
        return "Hybrid"
    if "on-site" in all_text or "onsite" in all_text:
        return "On-site"
    return None


def scrape_linkedin_jobs(search_keyword, location, target_jobs=None, delay_range=(2, 6), timeout=15):
    """Scrape LinkedIn guest search pages"""
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    search_keyword = (
    # ====== Core Software & Development ======
    "software OR backend OR frontend OR full stack OR web OR mobile OR android OR ios OR "
    "react OR node OR python OR java OR c# OR .net OR php OR ruby OR kotlin OR swift OR "
    "flutter OR react native OR angular OR vue OR typescript OR javascript OR go OR rust OR "
    "devops OR cloud OR sre OR infrastructure OR platform OR automation OR ci cd OR "
    "aws OR azure OR gcp OR kubernetes OR docker OR terraform OR ansible OR linux OR bash OR "
    "microservices OR api OR rest OR graphql OR serverless OR distributed systems OR scalability OR "

    # ====== Data, AI & ML ======
    "data OR analytics OR etl OR big data OR hadoop OR spark OR databricks OR powerbi OR tableau OR "
    "ml OR machine learning OR ai OR artificial intelligence OR deep learning OR nlp OR llm OR "
    "data science OR data engineer OR data scientist OR ml engineer OR mlops OR "
    "database OR sql OR nosql OR postgres OR mysql OR mongodb OR oracle OR snowflake OR redshift OR "

    # ====== Cybersecurity & Networking ======
    "security OR cybersecurity OR infosec OR appsec OR pentest OR penetration testing OR "
    "network OR sysadmin OR systems OR infrastructure OR vpn OR soc OR siem OR firewall OR "
    "identity OR iam OR compliance OR risk OR zero trust OR endpoint OR cloud security OR "

    # ====== Embedded, Hardware & IoT ======
    "embedded OR firmware OR iot OR robotics OR electronics OR fpga OR hardware OR "
    "arduino OR raspberry pi OR sensor OR automation engineer OR mechatronics OR control systems OR "

    # ====== Testing & QA ======
    "qa OR quality assurance OR test automation OR selenium OR cypress OR playwright OR testing OR "
    "manual testing OR performance testing OR regression testing OR test engineer OR test analyst OR "

    # ====== UI/UX & Product ======
    "ui OR ux OR ui/ux OR user interface OR user experience OR "
    "product design OR visual design OR interaction design OR design systems OR "
    "usability OR wireframe OR prototype OR figma OR sketch OR adobe xd OR invision OR zeplin OR "
    "product manager OR technical product manager OR scrum master OR agile coach OR "
    "service design OR design thinking OR human centered design OR accessibility OR "

    # ====== Graphic, Motion, 3D & Creative Tech ======
    "graphic design OR visual design OR motion design OR motion graphics OR animation OR illustrator OR photoshop OR "
    "after effects OR premiere OR indesign OR blender OR maya OR cinema 4d OR houdini OR 3ds max OR "
    "concept art OR digital art OR creative technologist OR creative developer OR multimedia OR "
    "game OR game design OR game developer OR unity OR unreal OR vr OR ar OR xr OR virtual reality OR augmented reality OR "
    "storyboard OR video editing OR content creation OR vfx OR sfx OR compositing OR visual effects OR "

    # ====== Blockchain, Web3 & Emerging Tech ======
    "blockchain OR crypto OR solidity OR smart contract OR web3 OR nft OR dapp OR defi OR metaverse OR "

    # ====== Support, Operations & General IT ======
    "support OR helpdesk OR service desk OR it support OR systems support OR technical support OR "
    "network engineer OR desktop support OR system administrator OR field engineer OR "
    "technician OR specialist OR consultant OR architect OR solution architect OR software architect OR systems architect OR "

    # ====== Experience Levels ======
    "lead OR senior OR junior OR intern OR entry level OR associate OR graduate OR trainee OR "
    "manager OR director OR head OR principal OR vp OR cto OR team lead OR mentor OR "
    "it OR tech OR technology OR information technology"
    )

    keyword_q = urllib.parse.quote_plus(search_keyword)
    location_q = urllib.parse.quote_plus(location)
    

    session = create_session()
    all_jobs = []
    start, page_num, jobs_found = 0, 0, 0
    seen_links = set()

    while True:
        url = f"{base_url}?keywords={keyword_q}&location={location_q}&f_TPR=r86400&start={start}"

        print(f"\nFetching page {page_num + 1} (start={start})...")
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
        except Exception as e:
            print(f"Error fetching page {page_num + 1}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        job_listings = soup.find_all("li")
        if not job_listings:
            print("No more job listings found. Ending scraping.")
            break

        print(f"Found {len(job_listings)} jobs on page {page_num + 1}")

        for job in job_listings:
            job_link_elem = job.find("a", href=True)
            if not job_link_elem:
                continue
            job_link = get_absolute_url(job_link_elem["href"])
            if not job_link or job_link in seen_links:
                continue
            seen_links.add(job_link)

            job_post = {"job_link": job_link}
            try:
                job_resp = session.get(job_link, timeout=timeout)
                job_resp.raise_for_status()
            except Exception as e:
                print(f"Error fetching job: {e}")
                continue

            job_soup = BeautifulSoup(job_resp.text, "html.parser")

            # Title
            title_tag = job_soup.find("h1", class_=re.compile("topcard__title")) or job_soup.find("h1")
            job_post["job title"] = clean_text(title_tag.get_text(strip=True)) if title_tag else None

            # Company
            company_elem = job_soup.find("a", href=re.compile(r"/company/"))
            job_post["company"] = clean_text(company_elem.get_text(strip=True)) if company_elem else None
            job_post["company url"] = get_absolute_url(company_elem["href"]) if company_elem else None

            # Location
            job_post["location"] = extract_location(job_soup, job_post["company"])

            # Full description
            job_post["job description"] = get_full_job_description(job_soup)
            # Clean description
            job_post["job description"] = clean_job_description(job_post["job description"])

            # Employment type
            job_post["employment type"] = extract_employment_type(job_soup)

            # Flexibility
            job_post["job flexibility"] = extract_job_flexibility(job_soup)

            all_jobs.append(job_post)
            jobs_found += 1
            print(f"Job {jobs_found}: {job_post['job title']} - {job_post['company']}")

            if target_jobs and jobs_found >= target_jobs:
                print("Reached target, stopping.")
                return pd.DataFrame(all_jobs)

            time.sleep(random.uniform(*delay_range))

        page_num += 1
        start += len(job_listings)
        time.sleep(random.uniform(2, 4))

    return pd.DataFrame(all_jobs)

def get_final_salaries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["mapped_onet_title"] = None
    df["onet_score"] = None
    df["mean_salary_egp"] = None
    df["median_salary_egp"] = None
    df["p10_salary_egp"] = None
    df["p90_salary_egp"] = None
    df["salary_datapoints"] = None

    # Always reset index first to align i with row positions
    df = df.reset_index(drop=True)

    for i, title in enumerate(df["job title"]):
        onet_match, score = map_to_onet(title)
        if not onet_match:
            continue

        hist = get_salary_histogram(onet_match)
        mean, median, percentiles, counter = aggregate_stats(hist)

        # Skip if histogram empty
        if mean is None or percentiles is None:
            continue

        # Assign safely (index-based)
        df.loc[i, "mapped_onet_title"] = onet_match
        df.loc[i, "onet_score"] = round(score or 0, 2)
        df.loc[i, "mean_salary_egp"] = round(mean / 12 * PPP, 2)
        df.loc[i, "median_salary_egp"] = round(median / 12 * PPP, 2)
        df.loc[i, "p10_salary_egp"] = round(percentiles[0] / 12 * PPP, 2)
        df.loc[i, "p90_salary_egp"] = round(percentiles[1] / 12 * PPP, 2)
        df.loc[i, "salary_datapoints"] = int(counter or 0)

        time.sleep(SLEEP_INTERVAL)

    return df


def main_scrape(job_count: int):
    # Test run
    df = scrape_linkedin_jobs("", "Egypt", target_jobs=job_count)         # write search in first argument, location in second , target_jobs third
    df = get_final_salaries(df)

    if df is not None and not df.empty:
        column_order = [
            "company",
            "company url",
            "location",
            "job_link",
            "job title",
            "job description",
            "employment type",
            "job flexibility",
            # v Added from the salary-matching script
            "mapped_onet_title",
            "onet_score",
            "mean_salary_egp",
            "median_salary_egp",
            "p10_salary_egp",
            "p90_salary_egp",
            "salary_datapoints",
        ]
        df = df.reindex(columns=column_order)

    return df