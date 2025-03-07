import requests
import xmltodict
from bs4 import BeautifulSoup
from celery import Celery

BASE_URL = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html"
XML_BASE_URL = "https://zakupki.gov.ru/epz/order/notice/printForm/viewXml.html?regNumber="

# Создаем приложение Celery, работающее в eager-режиме
app = Celery('tasks', broker='memory://', backend='rpc://', task_always_eager=True)


def fetch_tender_links(page_number):
    """Собирает ссылки на печатные формы тендеров с указанной страницы."""
    params = {"fz44": "on", "pageNumber": page_number}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'Cookie': '_ym_uid=1741253243877128803; _ym_d=1741253243; _ym_isad=2',
        'Sec-ch-ua': 'Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133'
    }

    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка загрузки страницы {page_number}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    # tender_links = [
    #     f"https://zakupki.gov.ru{link.get('href')}"
    #     for link in soup.select("div.w-space-nowrap.ml-auto.registry-entryheader-topicon a")
    #     if link.get("href") and "view.html?regNumber=" in link.get("href")
    # ]
    tender_links = []

    for entry in soup.select('.search-registry-entry-block.box-shadow-search-input'):
        link_tag = entry.select_one('.registry-entry__header-top__icon a[href*="printForm/view.html?regNumber="]')
        if link_tag:  # Проверяем, что ссылка найдена
            tender_links.append(f"https://zakupki.gov.ru{link_tag.get('href')}")

    return tender_links


def fetch_publish_date(tender_url):
    """Парсит XML-форму тендера и извлекает дату публикации."""
    reg_number = tender_url.split("regNumber=")[-1]
    xml_url = XML_BASE_URL + reg_number

    try:

        response = requests.get(xml_url, timeout=30)
        response.raise_for_status()
        data = xmltodict.parse(response.text)
        # publish_date = data.get("export", {}).get("contractNotice", {}).get("publishDTInEIS")

        publish_date = None
        # if 'ns7:epNotificationEF2020' in data:
        #     common_info = data['ns7:epNotificationEF2020'].get('commonInfo', {})
        #     if 'publishDTInEIS' in common_info:
        #         publish_date = common_info['publishDTInEIS']

        # # Проходим по ключам XML и ищем корневой элемент, начинающийся с "ns7:epNotification"
        # for key in data.keys():
        #     if key.startswith("ns7:epNotification"):
        #         common_info = data[key].get("commonInfo", {})
        #         publish_date = common_info.get("publishDTInEIS")
        #         break


        for key in data.keys():
            if "epNotification" in key:
                common_info = data[key].get("commonInfo", {})
                publish_date = common_info.get("publishDTInEIS")
                break  # Как только нашли, выходим из цикла


    except requests.RequestException as e:
        print(f"Ошибка загрузки XML {xml_url}: {e}")
        return tender_url, None
    except Exception as e:
        print(f"Ошибка парсинга XML {xml_url}: {e}")
        return tender_url, None

    return tender_url, publish_date


def __main__():
    """Основная функция запуска задач."""
    page_numbers = [1, 2]
    tender_urls = []

    # Сбор ссылок
    for page in page_numbers:
        tender_urls.extend(fetch_tender_links(page))

    # Парсинг XML
    results = [fetch_publish_date(url) for url in tender_urls]

    # Вывод результатов
    for url, date in results:
        print(f"{url} - {date}")


if __name__ == "__main__":
    __main__()
