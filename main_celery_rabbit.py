import requests
import xmltodict
from bs4 import BeautifulSoup
from celery import Celery, group

BASE_URL = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html"
XML_BASE_URL = "https://zakupki.gov.ru/epz/order/notice/printForm/viewXml.html?regNumber="

# Настраиваем Celery с брокером RabbitMQ
app = Celery('tasks', broker='pyamqp://guest@localhost//', backend='rpc://')

@app.task
def fetch_tender_links(page_number):
    """Задача для сбора ссылок на тендеры с указанной страницы."""
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
    tender_links = []

    for entry in soup.select('.search-registry-entry-block.box-shadow-search-input'):
        link_tag = entry.select_one('.registry-entry__header-top__icon a[href*="printForm/view.html?regNumber="]')
        if link_tag:
            tender_links.append(f"https://zakupki.gov.ru{link_tag.get('href')}")

    return tender_links


@app.task
def fetch_publish_date(tender_url):
    """Задача для парсинга XML формы тендера и извлечения даты публикации."""
    reg_number = tender_url.split("regNumber=")[-1]
    xml_url = XML_BASE_URL + reg_number

    try:
        response = requests.get(xml_url, timeout=30)
        response.raise_for_status()
        data = xmltodict.parse(response.text)

        publish_date = None
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

    if publish_date:
        publish_date = str(publish_date)[:10]

    return tender_url, publish_date


def main_task():
    """Основная задача для запуска сбора ссылок и парсинга XML."""
    page_numbers = [1, 2]

    # Параллельный сбор ссылок
    fetch_links_group = group(fetch_tender_links.s(page) for page in page_numbers)
    tender_links_results = fetch_links_group.apply_async().join()  # Ожидаем завершения всех задач
    tender_urls = [url for sublist in tender_links_results for url in sublist]  # Объединяем списки

    # Параллельный парсинг XML
    fetch_dates_group = group(fetch_publish_date.s(url) for url in tender_urls)
    results = fetch_dates_group.apply_async().join()  # Ожидаем завершения всех задач

    # Вывод результатов
    for url, date in results:
        print(f"{url} - {date}")


if __name__ == "__main__":
    main_task()
