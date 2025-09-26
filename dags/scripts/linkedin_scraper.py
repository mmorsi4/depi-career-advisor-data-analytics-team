import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import urllib.parse
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def extract_job_description(desc_div):
    """Extract job description from the description div"""
    if not desc_div:
        return None

    # Get full text content
    full_text = desc_div.get_text("\n", strip=True)
    
    # Try to extract main job description by looking for common patterns
    blocks = [b.strip() for b in re.split(r'\n{2,}', full_text) if b.strip()]
    if len(blocks) < 3:
        blocks = [b.strip() for b in full_text.split('\n') if b.strip() and len(b) > 15]
    
    # Clean and filter blocks
    blocks = [re.sub(r'\s+', ' ', block) for block in blocks if len(block) > 20]
    
    # Look for job description patterns (avoid requirements sections)
    job_patterns = [
        r'^(about the role|job summary|position overview|role overview|what you.{1,30}do|your role|responsibilities include)',
        r'^(key responsibilit|primary responsibilit|main responsibilit|core responsibilit)',
        r'^(in this position|in this role|as our|as a|the successful candidate will)',
        r'^(day.{1,10}to.{1,10}day|daily tasks|primary duties|main duties)'
    ]
    
    job_re = re.compile('|'.join(job_patterns), flags=re.I)
    
    # First, try to find explicit job description section
    for i, block in enumerate(blocks):
        if job_re.search(block):
            match = job_re.search(block)
            content = block[match.end():].strip(" :\t\n-–—")
            if len(content) > 30:
                return content
            elif i + 1 < len(blocks):
                return blocks[i + 1]
    
    # If no explicit section found, return first substantial block that doesn't look like requirements
    for block in blocks:
        block_lower = block.lower()
        # Skip blocks that look like requirements
        if re.search(r'\b(required|minimum|must have|bachelor|degree|years? of experience|qualifications|requirements)\b', block_lower):
            continue
        
        # Good indicators for job description
        if re.search(r'\b(responsible for|will be|you will|your role|position involves|we are looking|join our|opportunity to|work with|collaborate|develop|manage|lead|create|design|implement|team|project|department|company|organization)\b', block_lower):
            return block
        
        # If it's substantial content and we haven't found anything better
        if len(block) > 100:
            return block
    
    # Fallback: return the first few blocks combined
    if blocks:
        return ' '.join(blocks[:2])
    
    return None


def is_company_name(text, known_company_name=None):
    """Check if text looks like a company name rather than a location"""
    if not text:
        return False
    
    text_lower = text.lower()
    
    # If we know the company name, check if this text matches it
    if known_company_name and known_company_name.lower() in text_lower:
        return True
    
    # Common company suffixes and indicators
    company_indicators = [
        r'\b(inc|llc|ltd|corp|corporation|company|group|technologies|solutions|systems|services|consulting|holdings|enterprises|international|global|associates|partners)\b',
        r'\b(tech|software|digital|cyber|cloud|data|analytics|ai|ml|fintech|biotech|pharma|healthcare|medical|financial|insurance|banking|retail|logistics|manufacturing)\b',
        r'\b(startup|firm|agency|studio|lab|labs|research|development|innovation|ventures|capital)\b'
    ]
    
    for pattern in company_indicators:
        if re.search(pattern, text_lower):
            return True
    
    # Check for all caps (often company names)
    if text.isupper() and len(text) > 3:
        return True
    
    # Check for patterns that suggest company names
    if re.search(r'^[A-Z][a-z]+ [A-Z][a-z]+( [A-Z][a-z]+)*$', text) and not re.search(r'\b(remote|hybrid|on-?site)\b', text_lower):
        # Could be a company name if it's Title Case without location indicators
        return True
    
    return False


def is_valid_location(text):
    """Check if text looks like a valid location"""
    if not text or len(text) < 2:
        return False
    
    text_lower = text.lower()
    
    # Strong location indicators
    location_indicators = [
        r'\b(remote|hybrid|on-?site|work from home|wfh)\b',
        r'\b(city|county|state|province|country|region|area|district)\b',
        r'\b(cairo|alexandria|giza|egypt|dubai|riyadh|saudi|arabia|london|uk|new york|usa|los angeles|chicago|toronto|canada|berlin|germany|paris|france|tokyo|japan|sydney|australia)\b',
        r'[A-Za-z\s]+,\s*[A-Za-z\s]{2,}',  # City, Country/State format
    ]
    
    for pattern in location_indicators:
        if re.search(pattern, text_lower):
            return True
    
    # Check for common location patterns
    if re.search(r'^[A-Za-z\s]+,\s*[A-Za-z\s]{2,}$', text):  # City, State/Country
        return True
    
    return False


def extract_location(job_soup, company_name=None):
    """Extract job location information, filtering out company names"""
    
    # First, try to find location in job criteria section (most reliable)
    criteria_items = job_soup.find_all(['li', 'div'], class_=re.compile(r'job-criteria__item|job-details__text'))
    for item in criteria_items:
        # Look for items that contain location-related headers
        header = item.find(['h3', 'span', 'dt'], class_=re.compile(r'job-criteria__subheader|job-details__label'))
        if header and re.search(r'\b(location|workplace|where)\b', header.get_text().lower()):
            # Get the corresponding value
            value = item.find(['span', 'dd'], class_=re.compile(r'job-criteria__text|job-details__value'))
            if value:
                location_text = value.get_text(strip=True)
                if location_text and len(location_text) > 2 and not is_company_name(location_text, company_name):
                    return location_text
    
    # Second approach: Look for location in topcard area, but be more specific
    topcard_elements = job_soup.find_all(['span', 'div'], class_=re.compile(r'topcard__flavor'))
    for elem in topcard_elements:
        text = elem.get_text(strip=True)
        # Skip if it's a company name or other non-location content
        if (not is_company_name(text, company_name) 
            and not re.search(r'\b\d+\s*(hour|day|week|month|year)s?\s*ago\b', text.lower())
            and len(text) > 2
            and is_valid_location(text)):
            return text
    
    # Third approach: Look in structured job details
    details_sections = job_soup.find_all(['div', 'section'], class_=re.compile(r'(job-details|job-criteria)'))
    for section in details_sections:
        # Look for dt/dd pairs or similar structured content
        location_labels = section.find_all(['dt', 'h3', 'span'], string=re.compile(r'\b(location|workplace|where)\b', re.I))
        for label in location_labels:
            # Find the next element that contains the value
            next_elem = label.find_next(['dd', 'span', 'div'])
            if next_elem:
                location_text = next_elem.get_text(strip=True)
                if location_text and len(location_text) > 2 and not is_company_name(location_text, company_name):
                    return location_text
    
    # Fourth approach: Look for location patterns in the entire topcard section
    topcard = job_soup.find(['div', 'section'], class_=re.compile(r'topcard|job-header'))
    if topcard:
        # Get all text and look for location patterns
        all_text = topcard.get_text('\n', strip=True)
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
        
        for line in lines:
            # Skip obvious non-location lines and company names
            if (re.search(r'\b(posted|ago|apply|save|share|follow|job|position|role)\b', line.lower()) or
                re.search(r'^\d+\s*(applicant|connection|employee)', line.lower()) or
                len(line) < 3 or
                is_company_name(line, company_name)):
                continue
                
            # Look for valid location patterns
            if is_valid_location(line):
                return line
    
    # Fifth approach: Look in job description for location mentions
    desc_div = job_soup.find("div", class_=re.compile("show-more-less-html__markup"))
    if desc_div:
        desc_text = desc_div.get_text()
        # Look for "based in", "located in", "office in" patterns
        location_patterns = [
            r'(?:based|located|office)\s+in\s+([A-Za-z\s,]+?)(?:\.|,|\n|$)',
            r'(?:work\s+from|position\s+in|role\s+in)\s+([A-Za-z\s,]+?)(?:\.|,|\n|$)'
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, desc_text, re.I)
            if match:
                potential_location = match.group(1).strip()
                if (len(potential_location) > 2 and len(potential_location) < 50 
                    and not is_company_name(potential_location, company_name)
                    and is_valid_location(potential_location)):
                    return potential_location
    
    return None


def extract_employment_type(job_soup):
    """Extract employment type information"""
    # Look for employment type in various locations
    employment_patterns = [
        r'\b(full.?time|part.?time|contract|temporary|internship|freelance|remote|hybrid|on.?site)\b'
    ]
    
    # Check in job details section
    details_sections = job_soup.find_all(['div', 'span', 'li'], class_=re.compile(r'(job-details|topcard|criteria)'))
    for section in details_sections:
        text = section.get_text(strip=True).lower()
        for pattern in employment_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).replace('.', '-').title()
    
    # Check in description
    desc_div = job_soup.find("div", class_=re.compile("show-more-less-html__markup"))
    if desc_div:
        desc_text = desc_div.get_text().lower()
        for pattern in employment_patterns:
            match = re.search(pattern, desc_text)
            if match:
                return match.group(1).replace('.', '-').title()
    
    return None


def extract_job_flexibility(job_soup):
    """Extract job flexibility information (remote, hybrid, on-site)"""
    flexibility_patterns = [
        r'\b(remote|work from home|wfh|telecommute)\b',
        r'\b(hybrid|flexible|mix of remote)\b',
        r'\b(on.?site|office|in.?person)\b'
    ]
    
    flexibility_keywords = {
        'remote': ['remote', 'work from home', 'wfh', 'telecommute'],
        'hybrid': ['hybrid', 'flexible', 'mix of remote'],
        'on-site': ['on-site', 'onsite', 'office', 'in-person', 'in person']
    }
    
    # Check in job details and description
    all_text = job_soup.get_text().lower()
    
    for flexibility_type, keywords in flexibility_keywords.items():
        for keyword in keywords:
            if keyword in all_text:
                return flexibility_type.title()
    
    return None


def scrape_linkedin_jobs(search_keyword, location, target_jobs=None, delay_range=(2, 6), timeout=15):
    """
    Scrape job postings from LinkedIn guest search pages.
    Returns a pandas DataFrame with specified columns only.
    """
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    keyword_q = urllib.parse.quote_plus(search_keyword)
    location_q = urllib.parse.quote_plus(location)

    session = create_session()
    all_jobs = []
    jobs_found = 0
    start = 0
    page_num = 0
    processed_links = set()

    while True:
        url = f"{base_url}?keywords={keyword_q}&location={location_q}&start={start}"
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

        print(f"Found {len(job_listings)} items on page {page_num + 1}")

        for job in job_listings:
            # find first anchor (job link)
            job_link_elem = job.find("a", href=True)
            if not job_link_elem:
                continue
            job_link = get_absolute_url(job_link_elem["href"]) or job_link_elem["href"]

            # skip duplicates
            if job_link in processed_links:
                continue
            processed_links.add(job_link)

            job_post = {"job_link": job_link}
            try:
                job_resp = session.get(job_link, timeout=timeout)
                job_resp.raise_for_status()
            except Exception as e:
                print(f"Error fetching job detail {job_link}: {e}")
                continue

            job_soup = BeautifulSoup(job_resp.text, "html.parser")

            # Title
            title_tag = job_soup.find("h1", class_=re.compile("topcard__title")) or job_soup.find("h1")
            job_post["job title"] = title_tag.get_text(strip=True) if title_tag else None

            # Company: prefer a link with '/company/' in href or the topcard org link
            company_name = None
            company_link_elem = job_soup.find("a", href=re.compile(r"/company/")) or job_soup.find("a", class_=re.compile("topcard__org-name-link"))
            if company_link_elem:
                company_name = company_link_elem.get_text(strip=True)
                job_post["company"] = company_name
                job_post["company url"] = get_absolute_url(company_link_elem.get("href"))
            else:
                # fallback: plain text company name
                comp_span = job_soup.find("span", class_=re.compile("topcard__flavor"))
                company_name = comp_span.get_text(strip=True) if comp_span else None
                job_post["company"] = company_name
                job_post["company url"] = None

            # Location (now with company name filtering)
            job_post["location"] = extract_location(job_soup, company_name)

            # Description
            desc_div = job_soup.find("div", class_=re.compile("show-more-less-html__markup"))
            job_post["job description"] = extract_job_description(desc_div)

            # Employment type
            job_post["employment type"] = extract_employment_type(job_soup)

            # Job flexibility
            job_post["job flexibility"] = extract_job_flexibility(job_soup)

            all_jobs.append(job_post)
            jobs_found += 1

            print(f"Job {jobs_found}: {job_post.get('job title', 'Unknown')} - "
                  f"Company: {job_post.get('company', 'N/A')} - "
                  f"Location: {job_post.get('location', 'N/A')} - "
                  f"Type: {job_post.get('employment type', 'N/A')} - "
                  f"Flexibility: {job_post.get('job flexibility', 'N/A')}")

            if target_jobs and jobs_found >= target_jobs:
                print(f"Reached target of {target_jobs} jobs. Stopping.")
                df = pd.DataFrame(all_jobs)
                return df

            # polite delay between job detail requests
            time.sleep(random.uniform(delay_range[0], delay_range[1]))

        page_num += 1
        start += len(job_listings)
        # polite delay between pages
        time.sleep(random.uniform(2, 5))

    df = pd.DataFrame(all_jobs)

    # Data quality statistics
    if not df.empty:
        print("\n" + "="*50)
        print("DATA EXTRACTION STATISTICS")
        print("="*50)
        print(f"Total jobs scraped: {len(df)}")
        
        for col in ["company", "company url", "location", "job_link", "job title", "job description", "employment type", "job flexibility"]:
            if col in df.columns:
                count = df[col].notna().sum()
                percentage = (count/len(df))*100
                print(f"{col}: {count} / {len(df)} ({percentage:.1f}%)")
        print("="*50)

    return df

def main_scrape(job_count: int):
    # Test run
    df = scrape_linkedin_jobs("", "Egypt", target_jobs=job_count)         # write search in first argument, location in second , target_jobs third
    
    if df is not None and not df.empty:
        column_order = ["company", "company url", "location", "job_link", "job title", "job description", "employment type", "job flexibility"]
        df = df.reindex(columns=column_order)

    return df