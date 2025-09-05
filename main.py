import time
import logging

from yaml_reader import YamlPipelineExecutor
from utils.setup_logging import setup_logging

### Add-on with Queue for the threads so we can manage its speed and scale up and down
### https://realpython.com/python-concurrency/#threading-and-multiprocessing

def main():

    setup_logging()
    logger = logging.getLogger(__name__)
    pipeline_location = 'pipelines/wiki_yahoo_pipeline.yaml'   
    start_time = time.time() 
        
    try: 
        logger.info("Starting pipeline %s", pipeline_location)
        pipeline_executor = YamlPipelineExecutor(pipeline_location)
        pipeline_executor.start()    
        elapsed  = round(time.time() - start_time, 1)
        logger.info("Total time taken for running the pipeline: %s seconds", elapsed)     
    except Exception as e:
        logger.exception(f"Pipeline failed with except as {e}")
        raise

if __name__ == "__main__":
    main()
