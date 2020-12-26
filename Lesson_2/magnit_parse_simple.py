import os
import datetime as dt
import dotenv
import requests
from urllib.parse import urljoin
import bs4
import pymongo as pm

dotenv.load_dotenv('.env')
MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}


class MagnitParser:

    def __init__(self, start_url):
        self.start_url = start_url
        mongo_client = pm.MongoClient(os.getenv('DATA_BASE'))
        self.db = mongo_client['parse_11']

    def _get(self, url: str) -> bs4.BeautifulSoup:
        # todo обработока статусов и повторные запросы
        response = requests.get(url)
        return bs4.BeautifulSoup(response.text, 'lxml')

    def run(self):

        soup = self._get(self.start_url)
        for product in self.parse(soup):
            self.save(product)

    def parse(self, soup: bs4.BeautifulSoup) -> dict:
        catalog = soup.find('div', attrs={'class': 'сatalogue__main'})

        for product in catalog.findChildren('a'):
            try:
                pr_data = self.get_product(product)
            except AttributeError:
                continue
            yield pr_data

    def get_product(self, product_soup):
        dt_parser = self.date_parse(product_soup.find('div', attrs={'class': 'card-sale__date'}).text)

        product_template = {
            'url': lambda soups: urljoin(self.start_url, soups.attrs.get('href')),
            'promo_name': lambda soups: soups.find('div', attrs={'class': 'card-sale__header'}).text,

            'product_name': lambda soups: str(soups.find('div', attrs={'class': 'card-sale__title'}).text),

            'old_price': lambda soups: float(
                '.'.join(itm for itm in soups.find('div', attrs={'class': 'label__price_old'}).text.split())),

            'new_price': lambda soups: float(
                '.'.join(itm for itm in soups.find('div', attrs={'class': 'label__price_new'}).text.split())),

            'image_url': lambda soups: urljoin(self.start_url, soups.find('img').attrs.get('data-src')),
            'date_from': lambda _: next(dt_parser),
            'date_to': lambda _: next(dt_parser),
        }
        product_result = {}
        for key, value in product_template.items():
            try:
                product_result[key] = value(product_soup)
            except (AttributeError, ValueError, StopIteration):
                continue
        return product_result

    @staticmethod
    def date_parse(date_string: str):
        date_list = date_string.replace('с ', '', 1).replace('\n', '').split('до')
        for date in date_list:
            temp_date = date.split()
            yield dt.datetime(year=dt.datetime.now().year, day=int(temp_date[0]), month=MONTHS[temp_date[1][:3]])

    def save(self, data: dict):
        collection = self.db['magnit']
        collection.insert_one(data)


if __name__ == '__main__':
    parser = MagnitParser('https://magnit.ru/promo/?geo=moskva')
    parser.run()
