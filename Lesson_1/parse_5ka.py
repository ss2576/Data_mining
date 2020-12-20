"""
Источник: https://5ka.ru/special_offers/

Задача организовать сбор данных,
необходимо иметь метод сохранения данных в .json файлы

результат: Данные скачиваются с источника, при вызове метода/функции сохранения
 в файл скачанные данные сохраняются в Json вайлы, для каждой категории товаров
  должен быть создан отдельный файл и содержать товары исключительно
  соответсвующие данной категории.

пример структуры данных для файла:

{
"name": "имя категории",
"code": "Код соответсвующий категории (используется в запросах)",
"products": [{PRODUCT},  {PRODUCT}........] # список словарей товаров
соответсвующих данной категории
}
"""


import requests
import json
from pathlib import Path
import time

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"}
PARAMS = {"records_per_page": 50}
start_url = 'https://5ka.ru/api/v2/special_offers/'
category_url = 'https://5ka.ru/api/v2/categories/'



class StatusCodeError(Exception):
    def __init__(self, txt):
        self.txt = txt


class Parser5ka:
    headers = HEADERS
    params = PARAMS

    def __init__(self, start_url):
        self.start_url = start_url

    def run(self):
        try:
            for product in self.parse(self.start_url):
                file_path = Path(__file__).parent.joinpath('products', f'{product["id"]}.json')
                self.save(product, file_path)
        except requests.exceptions.MissingSchema:
            exit()

    def get_response(self, url, **kwargs):
        while True:
            try:
                response = requests.get(url, **kwargs)
                if response.status_code != 200:
                    raise StatusCodeError
                time.sleep(0.05)
                return response
            except (requests.exceptions.HTTPError,
                    StatusCodeError,
                    requests.exceptions.BaseHTTPError,
                    requests.exceptions.ConnectTimeout):
                time.sleep(0.25)

    def parse(self, url):
        if not url:
            url = self.start_url
        params = self.params
        while url:
            response = self.get_response(url, params=params, headers=self.headers)
            if params:
                params = {}
            data: dict = response.json()
            url = data.get('next')
            yield data.get('results')

    @staticmethod
    def save(data: dict, file_name):
        with open(f'products/{file_name}.json', 'w', encoding='UTF-8') as file:
            json.dump(data, file, ensure_ascii=False)


class ParserCatalog(Parser5ka):

    def __init__(self, start_url, category_url):
        self.category_url = category_url
        super().__init__(start_url)

    def get_categories(self, url):
        response = requests.get(url, headers=self.headers)
        return response.json()

    def run(self):
        for category in self.get_categories(self.category_url):
            data = {
                "name": category['parent_group_name'],
                'code': category['parent_group_code'],
                "products": [],
            }

            self.params['categories'] = category['parent_group_code']
            for products in self.parse(self.start_url):
                data["products"].extend(products)
                print(data["products"])
            self.save(
                data,
                category['parent_group_code'])


if __name__ == '__main__':
    parser = ParserCatalog(start_url, category_url)
    parser.run()
