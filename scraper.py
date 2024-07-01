import json
import re
import requests
import pandas as pd
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def tidy_element_text(element):
    s = element.get_text()
    return re.sub(r'\s+', ' ', s.strip())

def parse_table(table, department_id, cir_number, tour):
    results = []
    headers = [header.text.strip() for header in table.find_all('th')]
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        row_data = {
            'department_id': department_id,
            'cir_number': cir_number,
            'tour': tour
        }
        for header, column in zip(headers, columns):
            column_text = column.text.strip()
            row_data[header] = column_text
        if row_data['Liste des candidats'].startswith('M. '):
            row_data['gender'] = 'M'
        elif row_data['Liste des candidats'].startswith('Mme '):
            row_data['gender'] = 'F'
        results.append(row_data)
    return results

def scrape_cir(cir_url, department_id, cir_number):
    results = []
    logger.info(f"Scraping circonscription {cir_number} of department {department_id}")
    try:
        cir_html = requests.get(cir_url).content
        soup = BeautifulSoup(cir_html, 'html.parser')
        table = soup.find(class_="fr-table")
        if table is None:
            raise Exception(f"No results found in: {cir_url}")
        if table.find('caption').text.strip() == 'Candidatures* au 1er tour':
            logger.info(f'No results yet for circonscription {cir_number}')
            return None
        tour = '1'
        data = parse_table(table, department_id, cir_number, tour)
        results += data

        winners_found = sum(1 for r in results if r['Elu(e)'].strip() == 'Oui')
        if winners_found > 1:
            logger.error(f"{winners_found} winners found in: {json.dumps(data, indent=2, sort_keys=True)}")
            raise Exception(f"Unexpected number of winners found in {cir_url}")

    except Exception as e:
        logger.error(f"Error scraping {cir_url}: {e}")
    
    return results

def scrape_department(department_url, department_id):
    results = []
    try:
        department_html = requests.get(department_url).content
        soup = BeautifulSoup(department_html, 'html.parser')
        cir_options = soup.select('select#selectCir option')
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(scrape_cir, urljoin(department_url, option.get('value')), department_id, re.search(r"(\d{2})(?=/index\.html)", option.get('value')).group(1))
                for option in cir_options if option.get('value')
            ]
            for future in futures:
                result = future.result()
                if result:
                    results += result
    except Exception as e:
        logger.error(f"Error scraping department {department_id}: {e}")
    
    return results

def scrape_country(country_url):
    results = []
    try:
        country_html = requests.get(country_url).content
        soup = BeautifulSoup(country_html, 'html.parser')
        department_options = soup.select('select#selectDep option')
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(scrape_department, urljoin(country_url, option.get('value')), re.search(r'^(?:\d+|ZX|ZZ)', tidy_element_text(option)).group(0))
                for option in department_options if option.get('value')
            ]
            for future in futures:
                results += future.result()
    except Exception as e:
        logger.error(f"Error scraping country data: {e}")
    
    return results

# Scrape the data
data = scrape_country('https://www.resultats-elections.interieur.gouv.fr/legislatives2024/')

# Convert to DataFrame
df = pd.DataFrame(data)
print(df)

# Save to CSV (if needed)
df.to_csv('election_results.csv', index=False)
