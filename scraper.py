import json
import re
import requests
import pandas as pd
from urllib.parse import urljoin
from bs4 import BeautifulSoup


def tidy_element_text(element):
    s = element.get_text()
    return re.sub(r'\s+', ' ', s.strip())

def parse_table(table, department_id, cir_number, tour):
    results = []
    headers = [header.text.strip() for header in table.find_all('th')]
    # Iterate over rows in the table
    for row in table.find_all('tr')[1:]:  # Skip the header row
        columns = row.find_all('td')
        # Initialize a dictionary for row data
        row_data = {
            'department_id': department_id,
            'cir_number': cir_number,
            'tour': tour
        }
        # Add column data to row_data dictionary
        for header, column in zip(headers, columns):
            column_text = column.text.strip()
            row_data[header] = column_text
        # Add the row_data dictionary to results list
        if row_data['Liste des candidats'].startswith('M. '):
            row_data['gender'] = 'M'
        elif row_data['Liste des candidats'].startswith('Mme '):
            row_data['gender']  = 'F'
        results.append(row_data)
    return results


def scrape_cir(cir_url, department_id, cir_number):
    results = []
    print(f"Scraping de la {cir_number}e circonscription du {department_id}")
    cir_html = requests.get(cir_url).content
    soup = BeautifulSoup(cir_html, 'html.parser')
    table = soup.find(class_="fr-table")
    if table is None:
        raise Exception(f"No results found in: {cir_url}")
    if table.find('caption').text.strip() == 'Candidatures* au 1er tour':
        print('pas encore de résultats pour cette circonscription')
        return None
    tour = '1'
    data = parse_table(table, department_id, cir_number, tour)
    results += data

    # Make sure that exactly one person has elected set to 'Oui':
    winners_found = 0
    for r in results:
        elected = r['Elu(e)'].strip()
        if elected == 'Oui':
            winners_found += 1

    if winners_found > 1:
        print(f"{winners_found} winners found in:")
        print(json.dumps(data, indent=2, sort_keys=True))
        raise Exception(f"Unexpected number of winners found in {cir_url}")

    return results


def scrape_department(department_url, department_id):
    results = []
    department_html = requests.get(department_url).content
    soup = BeautifulSoup(department_html, 'html.parser')
    cir_options = soup.select('select#selectCir option')
    results = []
    for option in cir_options:
        cir_rel_url = option.get('value')
        if cir_rel_url == '':
            continue
        cir_number = re.search(r"(\d{2})(?=/index\.html)", cir_rel_url).group(1)
        cir_url = urljoin(department_url, cir_rel_url)
        result = scrape_cir(cir_url, department_id, cir_number)
        if result:
            results += result
    
    return results


def scrape_country(country_url):
    country_html = requests.get(country_url).content
    soup = BeautifulSoup(country_html, 'html.parser')
    department_options = soup.select('select#selectDep option')
    results = []
    for option in department_options:
        dep_rel_url = option.get('value')
        if dep_rel_url == '':
            continue
        department_id = re.search(r'^(?:\d+|ZX|ZZ)', tidy_element_text(option)).group(0)
        department_url = urljoin(country_url, dep_rel_url)
        results += scrape_department(department_url, department_id)
    
    return results

# Scrape the data
data = scrape_country('https://www.resultats-elections.interieur.gouv.fr/legislatives2024/')

# Convert to DataFrame
df = pd.DataFrame(data)
print(df)

# Save to CSV (if needed)
df.to_csv('election_results.csv', index=False)
