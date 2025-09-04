import threading
import requests
from bs4 import BeautifulSoup


class WikiWorkerScheduler(threading.Thread):
    
    def __init__(self, output_queues, **kwargs):
        if 'input_queue' in kwargs:
            kwargs.pop('input_queue')
        self._input_values = kwargs.pop('input_values')  
        print(f"a wiki input_values:{self._input_values}")    
        super(WikiWorkerScheduler, self).__init__(**kwargs)
        if 'input_queue' in kwargs:
            kwargs.pop('input_queue')
            
        # self._input_values = kwargs.pop('input_values') 
        print(f"b wiki input_values:{self._input_values}")                   
       
        temp_queue = output_queues
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queues = temp_queue        
        self.start()
    
    def run(self): 
        for entry in self._input_values:
            print(f'entry: {entry}')
            wikiWorker = WikiWorker(entry)
            symbol_count = 0
            for symbol in wikiWorker.get_sp_500_companies():
                for output_queue in self._output_queues:
                    output_queue.put(symbol)
                symbol_count += 1
                if symbol_count > 5:
                    break
        
        # caution: the below will cause the process stop prematurelly 
        # for output_queue in self._output_queues:
        #     for i in range(20):
        #         output_queue.put('DONE')
        

class WikiWorker(threading.Thread):
    def __init__(self, url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"):
        self._url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"     
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36"
        } 
        super(WikiWorker, self).__init__()
        self.start()

    @staticmethod
    def _extract_company_symbols(page_html):
        soup = BeautifulSoup(page_html, "lxml")  # ,'lxml'
        table = soup.find("table", {"id": "constituents"})
        table_body = table.find_all("tbody")
        for tbody in table_body:
            table_rows = tbody.find_all("tr")           
            for table_row in table_rows:
                cols = table_row.find_all("td")
                if cols:
                    symbol = cols[0].text.strip()
                    yield symbol

    def get_sp_500_companies(self):
        print(f"url: {self._url}")
        response = requests.get(self._url, headers=self._headers)        
        if response.status_code != 200:
            print("Couldn't get entries")
            return []

        yield from self._extract_company_symbols(response.text)
