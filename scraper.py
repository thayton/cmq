import os
import csv
import json
import time
import string
import logging
import argparse
import requests

from bs4 import BeautifulSoup
from urllib.parse import urljoin

class CmqOrgScraper(object):
    def __init__(self):
        self.url = 'http://www.cmq.org/bottin/index.aspx?lang=en&a=1'
        self.session = requests.Session()

        FORMAT = "[ %(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
        logging.basicConfig(format=FORMAT)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def csv_save(self, data):
        headers = [
            'Case Number',
            'Case Type',
            'Applicant',
            'Filing Date',
            'URL'
        ]

        with open('records.csv', 'w') as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(headers)

            for d in data:
                row = [
                    d.get('case-number', ''),
                    d.get('case-type', ''),
                    d.get('applicant', ''),
                    d.get('filing-date', ''),
                    d.get('url', ''),
                ]

                writer.writerow(row)
    
    def submit_search(self):
        pass

    def goto_next_page(self, soup):
        pass

    def get_search_form_data(self):
        resp = self.session.get(self.url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        form = soup.select_one('form#form1')
        data = {
            '__EVENTTARGET': None,
            '__EVENTARGUMENT': None
        }

        for i in form.find_all('input'):
            if i.get('name'):
                data[i['name']] = i.get('value')

        for s in form.find_all('select'):
            if s.get('name'):
                data[s['name']] = s.get('value')

        del data['cbxExMembres']
        del data['DDListSpecialite']

        return data

    def search_physician_names(self, names, load_from_file=True):
        links = []

        if os.path.exists('links.txt'):
            with open('links.txt', 'r') as fd:
                links = fd.read().splitlines()

            if len(links) > 0:
                return links

        data = self.get_search_form_data()

        for name in names:
            data['txbNom'] = name

            self.logger.info(f'Searching names with pattern {name}')

            resp = self.session.post('http://www.cmq.org/bottin/list.aspx', params={'lang': 'en'}, data=data)
            soup = BeautifulSoup(resp.text, 'html.parser')

            table = soup.select_one('table#GViewList')
            if table is None:
                self.logger.info(f'No search results for name pattern {name}')
                continue

            for a in table.select('tr > td > a'):
                url = urljoin(self.url, a['href'])
                if url not in links:
                    links.append(url)

                time.sleep(1)

        self.logger.info(f'Returning {len(links)} links')

        with open('links.txt', 'w') as fd:
            fd.write('\n'.join(links))

        return links

    def get_auto_complete_names(self, load_from_file=True):
        names = []

        if os.path.exists('names.txt'):
            with open('names.txt', 'r') as fd:
                names = fd.read().splitlines()

            if len(names) > 0:
                return names

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        for c in string.ascii_lowercase:
            self.logger.debug(f'Getting auto-complete names for names starting with {c}')

            auto_complete_url = 'http://www.cmq.org/bottin/index.aspx/GetAutocomplete'
            resp = self.session.post(auto_complete_url, headers=headers, json={
                'nom': c
            })
            data = resp.json()

            for name in data['d']:
                names.append(name)

            time.sleep(0.5)

        self.logger.info(f'Returning {len(names)} physician names to search')

        with open('names.txt', 'w') as fd:
            fd.write('\n'.join(names))

        return names

    def scrape(self):
        names = self.get_auto_complete_names()
        links = self.search_physician_names(names)

if __name__ == '__main__':
    scraper = CmqOrgScraper()
    scraper.scrape()
