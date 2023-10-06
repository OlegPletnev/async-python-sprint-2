from datetime import datetime, timedelta

from job import Job

from scheduler import Scheduler
from tasks import task_1, task_2, task_3


def main():

    scheduler = Scheduler()

    task1 = Job(func=task_1)
    task2 = Job(func=task_2, kwargs={'sleep': 1})
    task3 = Job(func=task_3, dependencies=[task1, task2],
                max_working_time=1,
                kwargs={'sleep': 2, 'txt': 'Текст-Текст-Текст'})
    start_at = (datetime.now() + timedelta(seconds=10)
                ).strftime('%d.%m.%Y %H:%M:%S')
    task4 = Job(func=task_2, start_at=start_at, kwargs={'sleep': 1})

    scheduler.add_task(task4)
    scheduler.add_task(task1)
    scheduler.add_task(task2)
    scheduler.add_task(task3)
    scheduler.add_task(task1)

    scheduler.stop()  # Штатное завершение работы

    scheduler.execute_tasks()


if __name__ == '__main__':
    main()