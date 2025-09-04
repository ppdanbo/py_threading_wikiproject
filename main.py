import time
from multiprocessing import Queue

from Workers.PostgresWorkers import PostgresMasterSchedule
from Workers.WikiWorker import WikiWorker
from Workers.YahooFinanceWorkers import YahooFinancePriceScheduler

from yaml_reader import YamlPipelineExecutor
### Add-on with Queue for the threads so we can manage its speed and scale up and down
### https://realpython.com/python-concurrency/#threading-and-multiprocessing


def main():

    pipeline_location = 'pipelines/wiki_yahoo_pipeline.yaml'    
    pipeline_executor = YamlPipelineExecutor(pipeline_location)
    pipeline_executor.process_pipeline()
  
    # symbol_queue = Queue()
    # postgres_queue = Queue()
    # process_queues = [postgres_queue]
    scraper_start_time = time.time()

    # wikiWorker = WikiWorker()
    # symbol_counter = 0
    # for symbol in wikiWorker.get_sp_500_companies():
    #     pipeline_executor._queues['SymbolQueue'].put(symbol)       
    #     symbol_counter += 1
    #     print(f"main put symbol {symbol} into symbol_queue with counter as {symbol_counter} ")
    #     # if symbol_counter >=5 :
    #     #     break
  
    for i in range(20):
        pipeline_executor._queues['SymbolQueue'].put('DONE')
   
    print("Total time taken for getting all the prices:", round(time.time() - scraper_start_time, 1), "seconds")
    pipeline_executor._join_workers() 

    print(
        "Total time taken for getting and storing all the prices:",
        round(time.time() - scraper_start_time, 1),
        "seconds",
    )

    exit


if __name__ == "__main__":
    main()
