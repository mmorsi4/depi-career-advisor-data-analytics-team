import requests
from bs4 import BeautifulSoup
import time
import random
import json
import html
from urllib.parse import quote
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class ScraperConfig:
    BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    JOBS_PER_PAGE = 25
    MIN_DELAY = 2
    MAX_DELAY = 5
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }


class LinkedInJobsScraper:
    def __init__(self):
        self.session = self._setup_session()

    def _setup_session(self):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def _build_search_url(self, keywords, location, f_TPR, start=0):
        params = {"keywords": keywords, "location": location, "f_TPR": f_TPR, "start": start}
        return f"{ScraperConfig.BASE_URL}?{'&'.join(f'{k}={quote(str(v))}' for k, v in params.items())}"

    def _fetch_job_page(self, url):
        response = self.session.get(url, headers=ScraperConfig.HEADERS)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _get_job_jsonld(self, job_url):
        try:
            soup = self._fetch_job_page(job_url)
            script_tag = soup.find("script", {"type": "application/ld+json"})
            if script_tag:
                data = json.loads(script_tag.string)

                # clean description
                if "description" in data:
                    clean = BeautifulSoup(html.unescape(data["description"]), "html.parser").get_text(" ", strip=True)
                    data["description"] = clean

                # keep job link
                data["job_link"] = job_url
                return data
        except Exception as e:
            print(f"Failed to fetch JSON-LD for {job_url}: {e}")
        return None

    def scrape_jobs(self, keywords, location, f_TPR, max_jobs=100):
        all_jobs = []
        start = 0
        while len(all_jobs) < max_jobs:
            url = self._build_search_url(keywords, location, f_TPR, start)
            soup = self._fetch_job_page(url)
            job_cards = soup.find_all("div", class_="base-card")

            if not job_cards:
                break

            for card in job_cards:
                try:
                    job_url = card.find("a", class_="base-card__full-link")["href"].split("?")[0]
                    job_data = self._get_job_jsonld(job_url)
                    if job_data:
                        all_jobs.append(job_data)
                        if len(all_jobs) >= max_jobs:
                            break
                except Exception:
                    continue

            print(f"Scraped {len(all_jobs)} jobs...")
            start += ScraperConfig.JOBS_PER_PAGE
            time.sleep(random.uniform(ScraperConfig.MIN_DELAY, ScraperConfig.MAX_DELAY))

        return all_jobs[:max_jobs]

    def save_results(self, jobs, filename="linkedin_jobs.json"):
        if jobs:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(jobs)} jobs to {filename}")


def main():
    params = {"keywords": "", "location": "Egypt", "f_TPR": "r86400", "max_jobs": 5}
    scraper = LinkedInJobsScraper()
    jobs = scraper.scrape_jobs(**params)
    scraper.save_results(jobs)


if __name__ == "__main__":
    main()