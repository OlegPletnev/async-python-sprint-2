from enum import Enum
from typing import Callable

from tasks import logger


class TaskStatus(str, Enum):
    WAITING = 'active pool'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SLEEPING = 'inactive pool'


class Job:
    def __init__(
            self,
            func: Callable,
            dependencies: list['Job'] | None = None,
            kwargs: dict | None = None,
            max_working_time: int = -1,
            start_at: str = '',
            tries: int = 0,

    ):
        self.func = func
        self.func_name = self.func.__name__
        self.kwargs = kwargs or {}
        self.dependencies = dependencies or []
        self.max_working_time = max_working_time
        self.tries = tries
        self.status = TaskStatus.SLEEPING
        self.start_at = start_at

    def _run(self) -> None:
        for _ in range(self.tries + 1):
            self.tries -= 1
            try:
                generator = self.run()
                next(generator)
                self.status = TaskStatus.COMPLETED
                break
            except Exception as e:
                logger.error('Задание не завершено: %s', e)
                logger.debug('Осталось %d попыток', self.tries)
                self.status = TaskStatus.FAILED

    def run(self):
        yield from self.func(**self.kwargs)
