import os
import csv
import json
import time
import string
import random
import logging
import argparse
import requests

from redis import StrictRedis
from redis.exceptions import RedisError
from rediscache import RedisCache
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

        self.init_cache()

    def delay(self):
        '''
        Put a random delay between 1-3 sec to avoid getting blocked by 
        the server and getting the 'exceeded the maximum number of queries
        per minute' warning
        '''
        time.sleep(random.randint(1,3))
        
    def csv_save(self, data):
        headers = [
            'Name',
            'Gender',
            'Permit Number',
            'Permit',
            'Status',
            'Insurance',
            'Specialty',
            'Activity',
            'Authorization',
            'Address',
            'Phone',
        ]

        with open('phsyicians.csv', 'w') as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(headers)

            for d in data:
                row = [
                    d.get('name', ''),
                    d.get('gender', ''),
                    d.get('permit number', ''),
                    d.get('permit', ''),
                    d.get('status', ''),
                    d.get('insurance', ''),
                    d.get('specialty', ''),
                    d.get('activity', ''),
                    d.get('authorization', ''),
                    d.get('address', ''),
                    d.get('phone', ''),
                ]

                writer.writerow(row)

    def init_cache(self):
        '''
        Attempt to connect to Redis (via ping) or set cache to None if that fails
        '''
        redis_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': 'foobared' 
        }

        client = StrictRedis(**redis_config)
        try:
            client.ping()
        except RedisError as ex:
            self.logger.warning(f'Failed to connect to Redis - {ex}')
            self.cache = None
        else:
            self.cache = RedisCache(client=client)

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

    def search_physician_names(self, names):
        physicians = []

        for name in names:
            data = self.get_search_form_data()            
            links = []
            
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
                links.append({ 'url': url, 'name': a.text.strip() })

            for link in links:
                data = None
                cached_details_url = f"http://www.cmq.org/bottin/details.aspx/{link['name']}"
                try:
                    text = self.cache[cached_details_url]
                    data = json.loads(text)
                except KeyError:
                    pass
                else:
                    self.logger.debug(f"Retrieved physician data for {link['name']} from cache")

                if data is None:
                    data = self.get_physician_info(link['url'])
                    if data != None:
                        self.cache[cached_details_url] = json.dumps(data)
                        physicians.append(data)
                
            self.delay()

        return physicians

    def get_auto_complete_names(self):
        names = []

        def post_ajax_name_auto_complete(prefix):
            auto_complete_url = 'http://www.cmq.org/bottin/index.aspx/GetAutocomplete'
            data = None

            try:
                text = self.cache[f'{auto_complete_url}/{prefix}']
                data = json.loads(text)
            except KeyError:
                pass
            else:
                self.logger.debug(f'Retrieved auto complete \'{prefix}\' from cache')

            if data is None:
                resp = self.session.post(auto_complete_url, json={ 'nom': prefix })                    
                data = resp.json()

                ival = [ 0.25, 0.5, 0.75 ]
                time.sleep(ival[random.randint(0,2)])

                self.cache[f'{auto_complete_url}/{prefix}'] = json.dumps(data)

            return data

        def get_auto_complete_names_r(prefix):
            for c in string.ascii_lowercase + '-':
                search_str = prefix + c

                self.logger.debug(f'Getting auto-complete names for names starting with {search_str}')
                data = post_ajax_name_auto_complete(search_str)

                for name in data['d']:
                    if name not in names:
                        names.append(name)

                if len(data['d']) >= 10:
                    get_auto_complete_names_r(search_str)

        get_auto_complete_names_r('')
        return names

    def get_physician_info(self, url):
        data = {}

        resp = self.session.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        table = soup.select_one('table.griddetails')

        if table is None:
            self.logger.warning(f'No physician info table at {url}')
            return None

        th = table.find('th')
        td = th.find_all('td')

        data['name'] = td[0].text.strip()

        for tr in table.find_all('tr')[3:]:
            td = tr.find_all('td')

            if len(td) != 2:
                continue

            k = td[0].text.strip().lower()
            v = td[1].text.strip()

            data[k] = v

        return data

    def scrape(self):
        names = self.get_auto_complete_names()
        physicians = self.search_physician_names(names)

        self.csv_save(physicians)

if __name__ == '__main__':
    scraper = CmqOrgScraper()
    scraper.scrape()
