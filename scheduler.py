import copy
import inspect
import pickle
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process
from threading import Thread, Timer

from job import Job, TaskStatus
from tasks import logger

SESSION_FILE = 'statement.pickle'


@dataclass(slots=True, frozen=True)
class Task:
    name: str
    job: Job


class Scheduler:
    def __init__(self, pool_size: int = 10, tasks: list[Task] = None):
        self.tasks = tasks or []
        self.pool_size = pool_size
        self.sleeping_tasks = deque()

    def add_tasks(self, jobs: list[Job]):
        pass

    def waiting_tasks(self):
        list_tasks = list(filter(
            lambda x: x.job.status not in [TaskStatus.COMPLETED,
                                           TaskStatus.FAILED],
            self.tasks))
        return list_tasks

    def stop(self):
        self.save_statement()
        self.load_statement()
        self.save_sleeping_statement()
        self.load_sleeping_statement()

    def add_task(self, job: Job):
        frame = inspect.currentframe()
        variables = frame.f_back.f_locals
        for name, value in variables.items():
            if value is job:
                main_task = Task(name=name, job=copy.deepcopy(job))
                main_task.job.status = TaskStatus.WAITING
                break
        if len(self.waiting_tasks()) < self.pool_size:
            if job.dependencies:
                if len(job.dependencies) + len(
                        self.waiting_tasks()) < self.pool_size:
                    for task in job.dependencies:
                        for name, value in variables.items():
                            if value is task:
                                _name = '.'.join([main_task.name, name])
                                subtask = Task(
                                    name=_name, job=copy.deepcopy(task)
                                )
                                logger.info(
                                    'Task <%s> (dependence) added',
                                    subtask.name
                                )
                                self.tasks.append(subtask)
                                task.status = TaskStatus.WAITING
                                break
            self.tasks.append(main_task)
            logger.info('Task <%s> added', main_task.name)
            # job.status = TaskStatus.WAITING
        else:
            self.sleeping_tasks.append(job)

    def execute_tasks(self):
        for task in self.waiting_tasks():
            time_lag = (formatted_date(
                task.job.start_at) - datetime.now()).total_seconds()
            if time_lag > 0:
                worker = Timer(interval=time_lag, function=task.job._run)
                logger.info(
                    'The "%s" task with a delayed timer starts', task.name
                )
                worker.start()
            elif task.job.max_working_time >= 0:
                worker = Process(target=task.job._run)
                logger.info(
                    'The time-limited task "%s" is started',
                    task.name
                )
                worker.start()
                worker.join(timeout=task.job.max_working_time)
                if worker.is_alive():
                    worker.terminate()
                    logger.debug(
                        'Task "%s" has been stopped. Time Limit Exceeded',
                        task.name
                    )
                    task.job.status = TaskStatus.FAILED
            else:
                worker = Thread(target=task.job._run)
                logger.info('Task "%s" is started', task.name)
                worker.start()
                worker.join()

    def save_statement(self):
        with open(SESSION_FILE, 'wb') as file:
            pickle.dump(self.tasks, file)

    def load_statement(self):
        with open(SESSION_FILE, 'rb') as file:
            saved_instances = pickle.load(file)
        self.tasks = saved_instances

    def save_sleeping_statement(self):
        self.sleeping_tasks = self.tasks
        with open(SESSION_FILE, 'wb') as file:
            pickle.dump(self.sleeping_tasks, file)

    def load_sleeping_statement(self):
        with open(SESSION_FILE, 'rb') as file:
            saved_instances = pickle.load(file)
        self.sleeping_tasks = saved_instances


def formatted_date(
        _date: str | None = None,
        str_format: str | None = '%d.%m.%Y %H:%M:%S'
):
    if not _date:
        return datetime.now()
    try:
        _start_at = datetime.strptime(_date, str_format)
        if _start_at >= datetime.now():
            return _start_at
    except Exception:
        logger.error('The time format in the "start_at" is incorrect.'
                     ' Reduced to now()')
        return datetime.now()
