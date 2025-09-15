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
    
    # Add realistic headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    # Add retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def detect_job_flexibility(text_content):
    """
    Detects job flexibility (Remote, Hybrid, On-site) from text content.
    
    :param text_content: str, combined text from job description and page
    :return: str, "Remote", "Hybrid", "On-site", or "UNDEFINED"
    """
    if not text_content:
        return "UNDEFINED"
    
    # Convert to lowercase for case-insensitive matching
    text_lower = text_content.lower()
    
    # Define patterns for each flexibility type
    remote_patterns = [
        r'\bremote\b', r'\bwork from home\b', r'\bwfh\b', r'\bhome office\b',
        r'\bdistributed team\b', r'\bfully remote\b', r'\b100% remote\b',
        r'\banywhere\b.*\bworld\b', r'\blocation independent\b',
        r'\bremote work\b', r'\bremote position\b', r'\bremote role\b',
        r'\bremote opportunity\b', r'\bremotely\b', r'\bfrom home\b'
    ]
    
    hybrid_patterns = [
        r'\bhybrid\b', r'\bflexible work\b', r'\bflex\b.*\bschedule\b',
        r'\bwork from home.*days\b', r'\bremote.*days\b', r'\boffice.*days\b',
        r'\bmixed\b.*\bremote\b', r'\bpart.*remote\b', r'\bpartly remote\b',
        r'\bflexible location\b', r'\bhybrid model\b', r'\bhybrid work\b',
        r'\bremote.*office\b', r'\boffice.*remote\b', r'\bblended work\b'
    ]
    
    onsite_patterns = [
        r'\bon-site\b', r'\bonsite\b', r'\bin-office\b', r'\boffice based\b',
        r'\boffice location\b', r'\bmust be located\b', r'\blocal candidate\b',
        r'\bin person\b', r'\bphysical presence\b', r'\bcommute\b',
        r'\brelocate\b', r'\boffice environment\b', r'\bfull.*office\b',
        r'\b100% office\b', r'\bfully onsite\b'
    ]
    
    # Count matches for each pattern type
    remote_score = sum(1 for pattern in remote_patterns if re.search(pattern, text_lower))
    hybrid_score = sum(1 for pattern in hybrid_patterns if re.search(pattern, text_lower))
    onsite_score = sum(1 for pattern in onsite_patterns if re.search(pattern, text_lower))
    
    # Determine flexibility based on scores
    if remote_score > hybrid_score and remote_score > onsite_score:
        return "Remote"
    elif hybrid_score > remote_score and hybrid_score > onsite_score:
        return "Hybrid"
    elif onsite_score > remote_score and onsite_score > hybrid_score:
        return "On-site"
    elif remote_score > 0 and hybrid_score > 0:
        return "Hybrid"  # If both remote and hybrid signals, lean towards hybrid
    elif remote_score > 0:
        return "Remote"
    elif hybrid_score > 0:
        return "Hybrid"
    elif onsite_score > 0:
        return "On-site"
    else:
        return "UNDEFINED"

def extract_job_sections_improved(desc_div):
    """
    Improved function to extract job sections with better pattern matching
    and fallback strategies to reduce None values.
    """
    if not desc_div:
        return None, None, None, None
    
    # Get full text with better formatting preservation
    full_text = desc_div.get_text("\n", strip=True)
    
    # Split into blocks with multiple strategies
    blocks = []
    
    # Strategy 1: Split by double newlines
    initial_blocks = [b.strip() for b in re.split(r'\n{2,}', full_text) if b.strip()]
    
    # Strategy 2: If we get very few blocks, try single newlines
    if len(initial_blocks) < 5:
        initial_blocks = [b.strip() for b in full_text.split('\n') if b.strip()]
    
    # Clean and filter blocks
    for block in initial_blocks:
        # Skip very short blocks (likely formatting artifacts)
        if len(block) > 10:
            # Clean excessive whitespace
            cleaned_block = re.sub(r'\s+', ' ', block)
            blocks.append(cleaned_block)
    
    # Enhanced regex patterns - more comprehensive and flexible
    major_heading_patterns = [
        r'^(Job\s+)?(Responsibilities|Qualifications|Requirements|Duties|Skills|Experience|What you.{1,20}do|Key Responsibilities|Primary Duties|Job Requirements|Role Responsibilities)',
        r'^(Essential|Minimum|Required|Basic|Must Have)\s+(Skills|Qualifications|Requirements|Experience)',
        r'^(Preferred|Desired|Nice to Have|Additional|Bonus)\s+(Skills|Qualifications|Requirements|Experience)',
        r'^(About|Company|Organization)\s+(the\s+)?(Role|Position|Job|Company|Organization|Team|Us)',
        r'^(Job\s+)?(Description|Summary|Overview)',
        r'^(Technical|Core|Main)\s+(Skills|Requirements|Responsibilities)'
    ]
    
    company_heading_patterns = [
        r'^(Company|About|Organization|Who are we|About us|Our company|The company|About the company)\b',
        r'^(Company\s+)?(Overview|Description|Profile|Background|Information)',
        r'^(Who\s+)?(we are|We Are)',
        r'^(Our\s+)?(Mission|Vision|Values|Culture|Story|Background)'
    ]
    
    required_patterns = [
        r'^(Required|Essential|Minimum|Must Have|Basic|Mandatory)\s*(Qualifications|Skills|Requirements|Experience)',
        r'^(You\s+)?(must|should|need to)\s+have',
        r'^(Minimum|Required)\s*(Requirements|Qualifications)',
        r'^\s*Requirements\s*:?\s*$',
        r'^\s*Qualifications\s*:?\s*$'
    ]
    
    preferred_patterns = [
        r'^(Preferred|Desired|Nice to Have|Bonus|Additional|Plus|Would be great)\s*(Qualifications|Skills|Requirements|Experience)',
        r'^(It would be|Would be)\s+(nice|great|a plus)',
        r'^(Bonus|Plus)\s+(points|if you)',
        r'^(Preferred|Desired)\s*(Requirements|Qualifications)'
    ]
    
    # Compile all patterns for efficiency
    major_heading_re = re.compile('|'.join(major_heading_patterns), flags=re.I)
    company_heading_re = re.compile('|'.join(company_heading_patterns), flags=re.I)
    required_re = re.compile('|'.join(required_patterns), flags=re.I)
    preferred_re = re.compile('|'.join(preferred_patterns), flags=re.I)
    
    # Find intro (everything before first major heading)
    first_heading_idx = None
    for i, block in enumerate(blocks):
        if major_heading_re.search(block):
            first_heading_idx = i
            break
    
    intro_blocks = blocks if first_heading_idx is None else blocks[:first_heading_idx]
    intro_text = " ".join(intro_blocks).strip()
    job_description = intro_text if intro_text else None
    
    # Enhanced company description extraction with multiple strategies
    company_description = None
    
    # Strategy 1: Look for company heading patterns
    for i, block in enumerate(blocks):
        if company_heading_re.search(block):
            # Check if content is in the same block
            match = company_heading_re.search(block)
            after_heading = block[match.end():].strip(" :\t\n-–—")
            
            if after_heading and len(after_heading) > 20:
                company_description = after_heading
                break
            else:
                # Look in subsequent blocks
                content_blocks = []
                j = i + 1
                while j < len(blocks):
                    next_block = blocks[j]
                    # Stop if we hit another major heading
                    if (major_heading_re.search(next_block) or 
                        company_heading_re.search(next_block) or
                        len(content_blocks) > 3):  # Don't collect too many blocks
                        break
                    content_blocks.append(next_block)
                    j += 1
                
                if content_blocks:
                    company_description = " ".join(content_blocks).strip()
                    break
    
    # Strategy 2: If no company section found, look for blocks with company-like keywords
    if not company_description:
        company_keywords = [
            'founded', 'established', 'leading', 'industry', 'mission', 'vision',
            'values', 'culture', 'team', 'employees', 'global', 'international',
            'headquarters', 'based in', 'specialize', 'focus on', 'dedicated to'
        ]
        
        for block in blocks[:10]:  # Check first 10 blocks only
            lower_block = block.lower()
            keyword_count = sum(1 for keyword in company_keywords if keyword in lower_block)
            
            # If block contains multiple company keywords and is substantial
            if keyword_count >= 2 and len(block) > 50:
                company_description = block
                break
    
    # Enhanced required qualifications extraction
    required_qualifications = None
    
    for i, block in enumerate(blocks):
        if required_re.search(block):
            match = required_re.search(block)
            after_heading = block[match.end():].strip(" :\t\n-–—")
            
            if after_heading and len(after_heading) > 15:
                required_qualifications = after_heading
                break
            else:
                # Collect subsequent blocks until next major heading
                content_blocks = []
                j = i + 1
                while j < len(blocks) and len(content_blocks) < 5:
                    next_block = blocks[j]
                    if (major_heading_re.search(next_block) or 
                        required_re.search(next_block) or
                        preferred_re.search(next_block)):
                        break
                    content_blocks.append(next_block)
                    j += 1
                
                if content_blocks:
                    required_qualifications = " ".join(content_blocks).strip()
                    break
    
    # If no explicit required section, look for blocks with requirement keywords
    if not required_qualifications:
        requirement_keywords = [
            'bachelor', 'master', 'degree', 'years of experience', 'experience in',
            'proficient', 'knowledge of', 'familiar with', 'understanding of',
            'ability to', 'must have', 'required', 'necessary'
        ]
        
        for block in blocks:
            lower_block = block.lower()
            keyword_count = sum(1 for keyword in requirement_keywords if keyword in lower_block)
            
            if keyword_count >= 2 and len(block) > 30:
                required_qualifications = block
                break
    
    # Enhanced preferred qualifications extraction
    preferred_qualifications = None
    
    for i, block in enumerate(blocks):
        if preferred_re.search(block):
            match = preferred_re.search(block)
            after_heading = block[match.end():].strip(" :\t\n-–—")
            
            if after_heading and len(after_heading) > 15:
                preferred_qualifications = after_heading
                break
            else:
                content_blocks = []
                j = i + 1
                while j < len(blocks) and len(content_blocks) < 5:
                    next_block = blocks[j]
                    if (major_heading_re.search(next_block) or 
                        preferred_re.search(next_block)):
                        break
                    content_blocks.append(next_block)
                    j += 1
                
                if content_blocks:
                    preferred_qualifications = " ".join(content_blocks).strip()
                    break
    
    # Look for preferred qualifications in blocks with specific keywords
    if not preferred_qualifications:
        preferred_keywords = [
            'preferred', 'desired', 'nice to have', 'bonus', 'plus',
            'would be great', 'additional experience', 'advantageous'
        ]
        
        for block in blocks:
            lower_block = block.lower()
            if any(keyword in lower_block for keyword in preferred_keywords) and len(block) > 30:
                preferred_qualifications = block
                break
    
    return job_description, company_description, required_qualifications, preferred_qualifications

def scrape_linkedin_jobs(search_keyword, location="Egypt", target_jobs=None, delay_range=(1, 3), timeout=30):
    """
    Enhanced LinkedIn job scraper with improved section extraction and more aggressive page fetching.
    
    :param search_keyword: str, e.g. "Data Scientist"
    :param location: str, e.g. "Egypt"
    :param target_jobs: int, target number of jobs to scrape (None = scrape all available)
    :param delay_range: tuple, random delay range between requests (min, max) in seconds
    :param timeout: int, request timeout in seconds
    :return: DataFrame with scraped jobs
    """

    # Encode keyword & location for URL
    keyword_encoded = urllib.parse.quote_plus(search_keyword)
    location_encoded = urllib.parse.quote_plus(location)

    all_jobs = []
    session = create_session()
    
    # Track processed job IDs to avoid duplicates
    processed_job_ids = set()

    print(f"Starting to scrape ALL jobs for '{search_keyword}' in '{location}' (last 24 hours)")
    if target_jobs:
        print(f"Target: {target_jobs} jobs")
    
    # Enhanced pagination parameters
    jobs_found = 0
    consecutive_failures = 0
    consecutive_empty_pages = 0
    max_consecutive_failures = 15  # Increased tolerance
    max_consecutive_empty_pages = 5  # Allow more empty pages before stopping
    page_num = 0
    max_pages = 200  # Maximum pages to attempt (LinkedIn typically has ~40 pages max)
    
    # Continue until we get all jobs or hit limits
    while page_num < max_pages:
        start_index = page_num * 25
        
        # LinkedIn job search URL with last 24 hours filter (f_TPR=r86400)
        url = (
            f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search'
            f'?keywords={keyword_encoded}&location={location_encoded}&f_TPR=r86400&start={start_index}&sortBy=DD'
        )
        
        try:
            response = session.get(url, timeout=timeout)
            print(f"Page {page_num + 1}: Status {response.status_code}")
            
            if response.status_code == 429:  # Rate limited
                wait_time = random.uniform(20, 45)  # Shorter wait times
                print(f"Rate limited. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    print("Too many consecutive failures. Stopping.")
                    break
                continue
                
            elif response.status_code != 200:
                print(f"Skipping page {page_num + 1}, status {response.status_code}")
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    print("Too many consecutive failures. Stopping.")
                    break
                # Move to next page even on failure to avoid getting stuck
                page_num += 1
                time.sleep(random.uniform(3, 8))
                continue
            
            consecutive_failures = 0  # Reset on success
            
            list_soup = BeautifulSoup(response.text, 'html.parser')
            page_jobs = list_soup.find_all('li')

            # Check if we have any job listings at all
            job_listings = [job for job in page_jobs if job.find('div', class_='base-card')]
            
            if not job_listings:
                consecutive_empty_pages += 1
                print(f"No jobs found on page {page_num + 1}. Empty pages: {consecutive_empty_pages}")
                
                if consecutive_empty_pages >= max_consecutive_empty_pages:
                    print("Reached the end of available jobs.")
                    break
                
                # Move to next page even if this one is empty
                page_num += 1
                time.sleep(random.uniform(2, 5))
                continue
            
            consecutive_empty_pages = 0  # Reset on finding jobs

            id_list = []
            for job in job_listings:
                base_card_div = job.find('div', class_='base-card')
                if not base_card_div:
                    continue
                urn = base_card_div.get('data-entity-urn')
                if not urn:
                    continue
                parts = urn.split(':')
                if len(parts) >= 4:
                    job_id = parts[3]
                    if job_id not in processed_job_ids:
                        id_list.append(job_id)
                        processed_job_ids.add(job_id)

            print(f"Found {len(id_list)} new jobs on page {page_num + 1} (Total unique jobs found: {len(processed_job_ids)})")
            
            if not id_list:
                consecutive_empty_pages += 1
                print(f"No new jobs found (all duplicates). Empty pages: {consecutive_empty_pages}")
                if consecutive_empty_pages >= max_consecutive_empty_pages:
                    print("No more new jobs available.")
                    break

            # Move to next page
            page_num += 1

            # Process jobs with reduced failure tolerance per page
            page_jobs_scraped = 0
            job_failures = 0
            max_job_failures_per_page = 8  # Allow more failures per page
            
            for job_id in id_list:
                # Check if we've reached our target
                if target_jobs and jobs_found >= target_jobs:
                    print(f"Reached target of {target_jobs} jobs!")
                    break
                    
                job_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}'
                
                try:
                    job_response = session.get(job_url, timeout=timeout)
                    
                    if job_response.status_code == 429:  # Rate limited
                        wait_time = random.uniform(30, 60)
                        print(f"Rate limited on job {job_id}. Waiting {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        # Retry the same job
                        try:
                            job_response = session.get(job_url, timeout=timeout)
                        except:
                            print(f"Retry failed for job {job_id}")
                            job_failures += 1
                            continue
                        
                    if job_response.status_code != 200:
                        print(f"Failed to fetch job {job_id}: {job_response.status_code}")
                        job_failures += 1
                        if job_failures >= max_job_failures_per_page:
                            print(f"Too many job failures on this page. Moving to next page.")
                            break
                        continue

                    job_failures = 0  # Reset on success
                    job_soup = BeautifulSoup(job_response.text, 'html.parser')
                    job_post = {}
                    job_post['job_id'] = job_id

                    # --- Company name ---
                    company_tag = job_soup.find('a', class_="topcard__org-name-link topcard__flavor--black-link")
                    if not company_tag:
                        company_tag = job_soup.find('span', class_="topcard__flavor")
                    job_post['company name'] = company_tag.get_text(strip=True) if company_tag else None

                    # --- Location ---
                    location_tag = job_soup.find('span', class_="topcard__flavor topcard__flavor--bullet")
                    job_post['job location'] = location_tag.get_text(strip=True) if location_tag else None

                    # --- Job title ---
                    title_tag = job_soup.find('h2', class_="top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title")
                    job_post['job title'] = title_tag.get_text(strip=True) if title_tag else None

                    # --- Employment type ---
                    criteria_tags = job_soup.find_all('span', class_="description__job-criteria-text description__job-criteria-text--criteria")
                    employment_type = None
                    for tag in criteria_tags:
                        text = tag.get_text(strip=True)
                        if any(word in text for word in ["Full-time", "Part-time", "Internship", "Contract", "Temporary"]):
                            employment_type = text
                            break
                    job_post['employment type'] = employment_type

                    # --- Collect all text content for flexibility detection ---
                    all_text_content = []
                    
                    # Add job title and location to text content
                    if job_post.get('job title'):
                        all_text_content.append(job_post['job title'])
                    if job_post.get('job location'):
                        all_text_content.append(job_post['job location'])

                    # --- Enhanced job description extraction ---
                    desc_div = job_soup.find('div', class_="show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden")
                    if desc_div:
                        # Use improved extraction function
                        job_desc, company_desc, required_qual, preferred_qual = extract_job_sections_improved(desc_div)
                        
                        job_post['job description'] = job_desc
                        job_post['company description'] = company_desc
                        job_post['required qualifications'] = required_qual
                        job_post['preferred qualifications'] = preferred_qual
                        
                        # Add to text content for flexibility detection
                        full_text = desc_div.get_text("\n", strip=True)
                        all_text_content.append(full_text)
                    else:
                        job_post['job description'] = None
                        job_post['company description'] = None
                        job_post['required qualifications'] = None
                        job_post['preferred qualifications'] = None

                    # --- Additional flexibility indicators ---
                    workplace_type_tags = job_soup.find_all('span', class_="description__job-criteria-text")
                    for tag in workplace_type_tags:
                        text = tag.get_text(strip=True)
                        all_text_content.append(text)

                    # Check any additional sections
                    additional_sections = job_soup.find_all(['div', 'span', 'p'], class_=re.compile(r'workplace|location|remote|hybrid|onsite'))
                    for section in additional_sections:
                        all_text_content.append(section.get_text(strip=True))

                    # --- Detect job flexibility ---
                    combined_text = " ".join(all_text_content)
                    job_post['job flexibility'] = detect_job_flexibility(combined_text)

                    all_jobs.append(job_post)
                    page_jobs_scraped += 1
                    jobs_found += 1
                    
                    print(f"Scraped job {jobs_found}: {job_post.get('job title', 'Unknown')} at {job_post.get('company name', 'Unknown')}")
                    
                    # Shorter delays for efficiency
                    time.sleep(random.uniform(delay_range[0], delay_range[1]))
                    
                except Exception as e:
                    print(f"Error scraping job {job_id}: {str(e)}")
                    job_failures += 1
                    if job_failures >= max_job_failures_per_page:
                        print(f"Too many job failures on this page. Moving to next page.")
                        break
                    continue

            print(f"Page {page_num} complete: {page_jobs_scraped} jobs scraped (Total: {jobs_found})")
            
            # Check if we've reached our target
            if target_jobs and jobs_found >= target_jobs:
                print(f"Successfully reached target of {target_jobs} jobs!")
                break
            
            # Shorter delays between pages for efficiency
            time.sleep(random.uniform(3, 8))
                
        except Exception as e:
            print(f"Error on page {page_num + 1}: {str(e)}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print("Too many consecutive failures. Stopping.")
                break
            page_num += 1  # Move to next page even on error
            time.sleep(random.uniform(5, 12))
            continue

    print(f"Scraping complete! Found {jobs_found} jobs total across {page_num} pages.")
    
    # Print statistics about data quality
    df = pd.DataFrame(all_jobs)
    if len(df) > 0:
        print(f"\nData Quality Statistics:")
        print(f"Company descriptions found: {df['company description'].notna().sum()} / {len(df)} ({df['company description'].notna().mean()*100:.1f}%)")
        print(f"Required qualifications found: {df['required qualifications'].notna().sum()} / {len(df)} ({df['required qualifications'].notna().mean()*100:.1f}%)")
        print(f"Preferred qualifications found: {df['preferred qualifications'].notna().sum()} / {len(df)} ({df['preferred qualifications'].notna().mean()*100:.1f}%)")
    
    return df

# Example usage:
if __name__ == "__main__":
    # Scrape  jobs in Egypt from the last 24 hours
    df = scrape_linkedin_jobs(
        search_keyword="", 
        location="Egypt", 
        target_jobs=None,  # Set to None to get ALL available jobs
        delay_range=(1, 3)  # Faster scraping with shorter delays
    )
    
    print(f"\nScraped {len(df)} jobs")
    if len(df) > 0:
        print(f"Unique companies: {df['company name'].nunique()}")
        print("\nJob flexibility distribution:")
        print(df['job flexibility'].value_counts())
        print("\nEmployment type distribution:")
        print(df['employment type'].value_counts())
        
        # Additional quality metrics
        print(f"\nData completeness:")
        for col in ['company description', 'required qualifications', 'preferred qualifications']:
            not_null_count = df[col].notna().sum()
            percentage = (not_null_count / len(df)) * 100
            print(f"{col}: {not_null_count}/{len(df)} ({percentage:.1f}%)")
    
    # Save to CSV
    df.drop(columns=['job_id'], inplace=True, errors='ignore')
    df.to_csv('linkedin_jobs_egypt.csv', index=False)
    print(f"\nResults saved to 'linkedin_jobs_optimized.csv'")
