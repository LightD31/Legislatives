import json
import re
import requests
import pandas as pd
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, List, Dict, Optional
from dataclasses import dataclass, asdict
from functools import partial

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ElectionResult:
    department_id: str
    cir_number: str
    tour: str
    position: int
    candidate: str
    elected: str
    nuance: str
    votes: str
    votes_pct_registered: str
    votes_pct_expressed: str
    gender: str

@dataclass
class Candidate:
    department_id: str
    cir_number: str
    name: str
    nuance: str
    gender: str

def tidy_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    return re.sub(r'\s+', ' ', text.strip())

def get_option_text(option: Tag) -> str:
    return tidy_text(option.string or option.text)

def parse_result_table(table: BeautifulSoup, department_id: str, cir_number: str, tour: str) -> List[ElectionResult]:
    results = []
    for position, row in enumerate(table.find_all('tr')[1:], start=1):
        columns = row.find_all('td')
        candidate = tidy_text(columns[0].text)
        gender = 'M' if candidate.startswith('M. ') else 'F' if candidate.startswith('Mme ') else ''
        results.append(ElectionResult(
            department_id=department_id,
            cir_number=cir_number,
            tour=tour,
            position=position,
            candidate=candidate,
            elected=tidy_text(columns[5].text),
            nuance=tidy_text(columns[1].text),
            votes=tidy_text(columns[2].text).replace(' ', ''),
            votes_pct_registered=str(float(tidy_text((columns[3].text).replace(',', '.')))/100.0).replace('.', ','),
            votes_pct_expressed=str(float(tidy_text((columns[4].text).replace(',', '.')))/100.0).replace('.', ','),
            gender=gender
        ))
    return results

def parse_candidates_table(table: BeautifulSoup, department_id: str, cir_number: str) -> List[Candidate]:
    return [
        Candidate(
            department_id=department_id,
            cir_number=cir_number,
            name=tidy_text(columns[0].text),
            nuance=tidy_text(columns[1].text),
            gender='M' if columns[0].text.strip().startswith('M. ') else 'F' if columns[0].text.strip().startswith('Mme ') else ''
        )
        for columns in [row.find_all('td') for row in table.find_all('tr')[1:]]
    ]

def scrape_cir(cir_url: str, department_id: str, cir_number: str) -> Tuple[List[ElectionResult], List[Candidate]]:
    logger.info(f"Scraping circonscription {cir_number} of department {department_id}")
    try:
        response = requests.get(cir_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        results = []
        candidates = []
        
        for table in soup.find_all(class_="fr-table"):
            caption = table.find('caption').text.strip().lower()
            if 'résultats' in caption:
                tour = '1' if '1er tour' in caption else '2' if '2nd tour' in caption else None
                if tour:
                    results.extend(parse_result_table(table, department_id, cir_number, tour))
            elif 'candidatures' in caption:
                candidates.extend(parse_candidates_table(table, department_id, cir_number))

        # Check for candidates marked as QUALIF T2 but not in candidates table
        candidate_names = {candidate.name for candidate in candidates}
        for result in results:
            if result.elected == 'QUALIF T2' and result.candidate not in candidate_names:
                result.elected = 'DESIST'

        winners = [r for r in results if r.elected.strip() == 'Oui']
        if len(winners) > 1:
            logger.error(f"Multiple winners found: {json.dumps([asdict(w) for w in winners], indent=2)}")
            raise ValueError(f"Unexpected number of winners found in {cir_url}")

        return results, candidates

    except requests.RequestException as e:
        logger.error(f"Error fetching {cir_url}: {e}")
    except Exception as e:
        logger.error(f"Error scraping {cir_url}: {e}")
    
    return [], []

def scrape_department(department_url: str, department_id: str) -> Tuple[List[ElectionResult], List[Candidate]]:
    try:
        response = requests.get(department_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        cir_options = soup.select('select#selectCir option')
        
        with ThreadPoolExecutor() as executor:
            futures = []
            for option in cir_options:
                if option.get('value'):
                    cir_url = urljoin(department_url, option['value'])
                    cir_number = re.search(r"(\d{2})(?=/index\.html)", option['value']).group(1)
                    futures.append(executor.submit(scrape_cir, cir_url, department_id, cir_number))
                else:
                    logger.info(f"Skipping option without 'value' attribute: {option}")
            
            results = []
            candidates = []
            for future in as_completed(futures):
                r, c = future.result()
                results.extend(r)
                candidates.extend(c)
        
        return results, candidates

    except requests.RequestException as e:
        logger.error(f"Error fetching department {department_id}: {e}")
    except Exception as e:
        logger.error(f"Error scraping department {department_id}: {e}", exc_info=True)
    
    return [], []

def scrape_country(country_url: str) -> Tuple[List[ElectionResult], List[Candidate]]:
    try:
        response = requests.get(country_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        department_options = soup.select('select#selectDep option')
        
        if not department_options:
            logger.error(f"No department options found at {country_url}. HTML content: {soup.prettify()[:500]}...")
            return [], []
        
        with ThreadPoolExecutor() as executor:
            futures = []
            for option in department_options:
                if option.get('value'):
                    department_url = urljoin(country_url, option['value'])
                    department_id_match = re.search(r'^(?:\d+|ZX|ZZ)', get_option_text(option))
                    if department_id_match:
                        department_id = department_id_match.group(0)
                        futures.append(executor.submit(scrape_department, department_url, department_id))
                    else:
                        logger.warning(f"Could not extract department ID from option: {get_option_text(option)}")
                else:
                    logger.info(f"Skipping option without 'value' attribute: {option}")
            
            results = []
            candidates = []
            for future in as_completed(futures):
                try:
                    r, c = future.result()
                    results.extend(r)
                    candidates.extend(c)
                except Exception as e:
                    logger.error(f"Error processing future: {e}")
        
        return results, candidates

    except requests.RequestException as e:
        logger.error(f"Error fetching country data: {e}")
    except Exception as e:
        logger.error(f"Error scraping country data: {e}", exc_info=True)
    
    return [], []

def main():
    country_url = 'https://www.resultats-elections.interieur.gouv.fr/legislatives2024/'
    results_data, candidates_data = scrape_country(country_url)

    if not results_data and not candidates_data:
        logger.error("No data was scraped. Exiting.")
        return

    results_df = pd.DataFrame([asdict(r) for r in results_data])
    candidates_df = pd.DataFrame([asdict(c) for c in candidates_data])

    print("Results Data:")
    print(results_df)
    print("\nCandidates Data:")
    print(candidates_df)

    results_df.to_csv('election_results.csv', index=False)
    candidates_df.to_csv('candidates_data.csv', index=False)

if __name__ == "__main__":
    main()