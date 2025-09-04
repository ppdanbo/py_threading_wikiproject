import importlib
import random
import time
import threading
from multiprocessing import Queue

import yaml

class YamlPipelineExecutor(threading.Thread):
    
    def __init__(self, pipeline_location):
        super(YamlPipelineExecutor, self).__init__()
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}
        self._queue_consumers = {}    # how many workers will consume that queue
        self._downstream_queues = {}   # what queues the workers will put to
            

    def _load_pipeline(self):
        with open(self._pipeline_location, "r") as infile:
            self._yaml_data = yaml.safe_load(infile)


    def _init_queues(self):
        for queue in self._yaml_data["queues"]:
            queue_name = queue["name"]
            self._queues[queue_name] = Queue()


    def _init_workers(self):
        for worker in self._yaml_data["workers"]:
            workerClass = getattr(importlib.import_module(worker["location"]), worker["class"])
            input_queue = worker.get("input_queue", None)
            output_queues = worker.get("output_queues", None)
            worker_name = worker.get("name")
            num_instances = worker.get("instances", 1)                      
            
            # track the name of the queues
            self._downstream_queues[worker_name] = output_queues
            
            # need to know how many workers will consume the input_queue
            if input_queue is not None:
                self._queue_consumers[input_queue] = num_instances
            
            input_params = {                          
                "input_queue": self._queues[input_queue] if input_queue is not None else None,
                "output_queues": (
                    [self._queues[output_q] for output_q in output_queues] \
                        if output_queues is not None else None
                ),
            }
            input_values = worker.get('input_values')
            if input_values is not None: 
                input_params['input_values'] = input_values
            self._workers[worker_name] = []
            for i in range(num_instances):
                self._workers[worker_name].append(workerClass(**input_params))

    def _join_workers(self):
        for worker_name in self._workers:
            for worker_thread in self._workers[worker_name]:
                worker_thread.join()

    def process_pipeline(self):
        ## init queues
        self._load_pipeline()
        self._init_queues()
        self._init_workers()
        # self._join_workers()  # this will block if run
        
    def run(self):
        self.process_pipeline()
        worker_stats = []
        while True:
            # monitor threads
            for worker_name in self._workers:
                total_worker_threads_alive = 0
                for worker_thread in self._workers[worker_name]:
                    if worker_thread.is_alive():
                        total_worker_threads_alive += 1
                        
                if total_worker_threads_alive == 0:
                    if self._downstream_queues[worker_name] is not None:
                        for output_queue in self._downstream_queues[worker_name]:
                            number_of_consumers = self._queue_consumers[output_queue]
                            for i in range(number_of_consumers):
                                self._queues[output_queue].put('DONE')
                    del self._workers[worker_name]   # skip the track once done   
            
                worker_stats.append(worker_name, total_worker_threads_alive)
               
            print(f"worker_stats:{worker_stats}")
            if len(self._workers) == 0:
                    break
            time.sleep(random.random()*5)

            