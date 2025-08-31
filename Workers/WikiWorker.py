import threading
import requests
from bs4 import BeautifulSoup


class WikiWorker(threading.Thread):
    def __init__(self, url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"):
        self._url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"      
        super(WikiWorker, self).__init__()
        self.start()

    @staticmethod
    def _extract_company_symbols(page_html):
        soup = BeautifulSoup(page_html, "lxml")  # ,'lxml'
        table = soup.find("table", {"id": "constituents"})
        table_body = table.find_all("tbody")
        for tbody in table_body:
            # print(f"tbody: {tbody}")
            table_rows = tbody.find_all("tr")
            # print(f"table_rows: {table_rows}")

            for table_row in table_rows:
                cols = table_row.find_all("td")
                if cols:
                    symbol = cols[0].text.strip()
                    yield symbol

    def get_sp_500_companies(self):
        print(f"url: {self._url}")
        response = requests.get(self._url)
        # print(f"response.status_code: {response.status_code}")
        if response.status_code != 200:
            print("Couldn't get entries")
            return []

        yield from self._extract_company_symbols(response.text)
