from dask.distributed import SchedulerPlugin, WorkerPlugin
import logging
import time

class TaskTimingPlugin(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.task_durations = {}

    def transition(self, key, start, finish, *args, stimulus_id, **kwargs):
        ts = self.scheduler.tasks[key]  # Get full TaskState
        print(f"Scheduler Transition: {key} from {start} to {finish}")
        if start == 'released' and finish == 'waiting':
            self.task_durations[key] = {'submitted': time.time()}
        elif start == 'processing' and finish == 'memory':
            self.task_durations[key]['start_processing'] = time.time()
        elif start == 'memory' and finish in ['released', 'forgotten']:
            self.task_durations[key]['end_processing'] = time.time()
            duration = (self.task_durations[key]['end_processing'] - self.task_durations[key]['start_processing'])
            logging.info(f"Task {key} took {duration:.4f} seconds to process.")

    def get_task_durations(self):
        return self.task_durations

class WorkerTimingPlugin(WorkerPlugin):
    def __init__(self):
        self.task_durations = {}

    def setup(self, worker):
        self.worker = worker

    def transition(self, key, start, finish, *args, stimulus_id, **kwargs):
        print(f"Worker Transition: {key} from {start} to {finish}")
        if start == 'waiting' and finish == 'ready':
            self.task_durations[key] = {'received_by_worker': time.time()}
        elif start == 'executing' and finish == 'memory':
            self.task_durations[key]['start_execution'] = time.time()
        elif start == 'memory' and finish == 'released':
            self.task_durations[key]['end_execution'] = time.time()
            duration = (self.task_durations[key]['end_execution'] - self.task_durations[key]['start_execution'])
            logging.info(f"Task {key} took {duration:.4f} seconds to execute.")

    def get_task_durations(self):
        return self.task_durations
