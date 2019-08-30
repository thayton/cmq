import re
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
        self.auto_complete_url = 'http://www.cmq.org/bottin/index.aspx/GetAutocomplete'
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

    def search_physician_name(self, name):
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

        data['txbNom'] = 'Aaron'

        resp = self.session.post('http://www.cmq.org/bottin/list.aspx', params={'lang': 'en'}, data=data)
        soup = BeautifulSoup(resp.text, 'html.parser')

        print(soup.prettify())

    def get_auto_complete_names(self):
        names = []

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        for c in string.ascii_lowercase:
            self.logger.debug(f'Getting auto-complete names for names starting with {c}')

            resp = self.session.post(self.auto_complete_url, headers=headers, json={
                'nom': c
            })
            data = resp.json()

            for name in data['d']:
                names.append(name)

            time.sleep(0.5)

        self.logger.info(f'Returning {len(names)} physician names to search')
        return names

    def scrape(self):
        names = self.get_auto_complete_names()

if __name__ == '__main__':
    scraper = CmqOrgScraper()
    scraper.scrape()
