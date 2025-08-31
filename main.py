import time

from multiprocessing import Queue
from Workers.WikiWorker import WikiWorker
from Workers.YahooFinanceWorkersMaster import YahooFinancePriceScheduler
from Workers.PostgresWorker import PostgresMasterSchedule

### Add-on with Queue for the threads so we can manage its speed and scale up and down
### https://realpython.com/python-concurrency/#threading-and-multiprocessing


def main():

    # make queue threading safe
    symbol_queue = Queue()
    postgres_queue = Queue()
    scraper_start_time = time.time()

    wikiWorker = WikiWorker()

    yahoo_finance_scheduler_threads = []
    num_yahoo_finance_price_workers = 15
    for i in range(num_yahoo_finance_price_workers):
        print(f"main creating YahooFinancePriceScheduler thread {i}")
        yahooFinancePriceScheduler = YahooFinancePriceScheduler(input_queue=symbol_queue, output_queue=postgres_queue)
        yahoo_finance_scheduler_threads.append(yahooFinancePriceScheduler)


    postgres_schedule_threads = []
    num_postgres_schedule_workers = 2
    for i in range(num_postgres_schedule_workers):
        print(f"main creating PostgresMasterSchedule thread {i}")
        postgresScheduler = PostgresMasterSchedule(input_queue=postgres_queue)
        postgres_schedule_threads.append(postgresScheduler)


    for symbol in wikiWorker.get_sp_500_companies():
        symbol_queue.put(symbol)

    for i in range(len(yahoo_finance_scheduler_threads)):
        symbol_queue.put("DONE")  # signal end of input  for every scheduler thread

    for i in range(len(yahoo_finance_scheduler_threads)):
        yahoo_finance_scheduler_threads[i].join()
    print("Total time taken for getting all the prices:", round(time.time() - scraper_start_time, 1), "seconds")
    
    exit


if __name__ == "__main__":
    main()
