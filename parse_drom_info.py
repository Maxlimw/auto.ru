from time import sleep
import re
import argparse
from pathlib import Path
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def parse_auto_page(page_url: str) -> dict[str, str]:
    req = requests.get(page_url)
    parsed = BeautifulSoup(req.content, 'html.parser')
    all_tables = parsed.findAll('table')
    if not all_tables:
        return {}
    car_info = all_tables[0]
    data = {part_info.find('th').text: part_info.find('td').text.replace('\xa0', ' ') for part_info in car_info.findAll('tr') if part_info.find('th')}
    data['page_title'] = [s.text for s in parsed.select('h1 > span')][0]
    data['price'] = [s.text.replace('\xa0', ' ') for s in parsed.select('.wb9m8q0')][0]
    pub_date = parsed.select('.css-pxeubi.evnwjo70')
    if pub_date:
        dates = re.findall(r'\d\d\.\d\d.\d\d\d\d', pub_date[0].text)
        if dates:
            data['publication_date'] = dates[0]
    return data


def get_all_links(page_url: str) -> list[dict[str, str]]:
    r = requests.get(page_url)
    r.raise_for_status()
    parsed_list = BeautifulSoup(r.content, 'html.parser')
    car_data = []
    for car_page in parsed_list.select('a>h3'):
        car_info = {
            'title': car_page.text,
            'link': car_page.parent.attrs['href']
        }
        car_data.append(car_info)
    return car_data


def get_all_model_links(base_url: str, max_page: int = 100) -> list[dict[str, str]]:
    car_links = []
    for i in tqdm(range(1, max_page)):
        parsed = get_all_links(base_url + f'/page{i}/')
        if len(parsed) == 0:
            print('No more cars to parse')
            break
        sleep(1)
        car_links.extend(parsed)
    return car_links


def enrich_with_page_info(car_links: list[dict[str, str]], timeout: int = 1) -> list[dict[str, str]]:
    for car_link in tqdm(car_links):
        car_link.update(parse_auto_page(car_link['link']))
        sleep(timeout)
    return car_links


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url", help="Базовая страница, с которой начинаем поиск")
    parser.add_argument("save_file", help="csv файл, в который сохраним результаты")
    parser.add_argument("--n_pages", type=int, default=100, help="Максимальное число страниц")
    parser.add_argument("--timeout", type=int, default=1, help="Число секунд между скачиваниями")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    print(f'Получаем список машин со страниц {args.base_url}')
    car_links = get_all_model_links(args.base_url, args.n_pages)
    print(f'Получено {len(car_links)} машин, начинаем скачивать по ним информацию')
    car_info = enrich_with_page_info(car_links, timeout=args.timeout)
    print(f'Сохраняем результаты в {args.save_file}')
    save_path = Path(args.save_file)
    pd.DataFrame(car_info).to_csv(save_path)
