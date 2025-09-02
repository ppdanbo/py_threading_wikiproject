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
    process_queues = [postgres_queue]
    scraper_start_time = time.time()

    wikiWorker = WikiWorker()

    yahoo_finance_scheduler_threads = []
    num_yahoo_finance_price_workers = 15
    for i in range(num_yahoo_finance_price_workers):
        print(f"main creating YahooFinancePriceScheduler thread {i}")
        yahooFinancePriceScheduler = YahooFinancePriceScheduler(input_queue=symbol_queue, output_queue=process_queues)
        yahoo_finance_scheduler_threads.append(yahooFinancePriceScheduler)


    postgres_schedule_threads = []
    num_postgres_schedule_workers = 2
    for i in range(num_postgres_schedule_workers):
        print(f"main creating PostgresMasterSchedule thread {i}")
        postgresScheduler = PostgresMasterSchedule(input_queue=postgres_queue)
        postgres_schedule_threads.append(postgresScheduler)

    symbol_counter = 0
    for symbol in wikiWorker.get_sp_500_companies():
        symbol_queue.put(symbol)
        symbol_counter += 1
        print(f"main put symbol {symbol} into symbol_queue with counter as {symbol_counter} ")
        # if symbol_counter >=5 :
        #     break
    
      
    for i in range(len(yahoo_finance_scheduler_threads)):
        yahoo_finance_scheduler_threads[i].join()    
    print("Total time taken for getting all the prices:", round(time.time() - scraper_start_time, 1), "seconds")      
        
    for i in range(len(postgres_schedule_threads)):
        postgres_schedule_threads[i].join() 
    postgres_queue.put(("DONE", None, None))     
    
    print("Total time taken for getting and storing all the prices:", round(time.time() - scraper_start_time, 1), "seconds")
    
    exit

if __name__ == "__main__":
    main()
