import inspect
import logging
from pathlib import Path
from typing import Generator

import requests
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def task_1():
    func_name = inspect.currentframe().f_code.co_name
    logger.info('The following function is executed "%s"', func_name)
    yield


def task_2(sleep: int = 2):
    func_name = inspect.currentframe().f_code.co_name
    logger.info('The "%s" function is executed with a sleep of %d seconds',
                func_name, sleep)
    time.sleep(sleep)
    yield


def task_3(sleep: int = 2, txt: str | None = None):
    func_name = inspect.currentframe().f_code.co_name
    logger.info('The "%s" function is executed with sleep %d seconds '
                'and text <%s>',
                func_name, sleep, txt)
    time.sleep(sleep)
    yield


def task_5():
    func_name = inspect.currentframe().f_code.co_name
    logger.info('The following function is executed "%s"', func_name)


def create_folder(path: str) -> Generator:
    try:
        Path(path).mkdir()
        logger.info('Create a folder <%s>. Job DONE', path)
    except FileExistsError as er:
        logger.info('Create a folder <%s>. Job COMPLETED (firstly)', path)
    except OSError as e:
        logger.error('Folder creation error: %s', e)
        raise Exception(
            'Check the name of the folder you want to create. Error: %s', e
        )
    finally:
        yield


def remove_folder(path: str) -> Generator:
    try:
        Path(path).rmdir()
        logger.info('Delete the %s folder. Job COMPLETE', path)
        yield
    except FileNotFoundError:
        logger.info('Delete the <%s> folder. Job DONE (Does Not Exist)',
                    path)
        yield
    except PermissionError as e:
        logger.error('No permissions to delete a folder: %s', e)
        raise Exception(e)
    except NotADirectoryError as e:
        logger.error('The specified path %s is not a folder', path)
        raise Exception(e)


def rename_folder_or_file(old: str, new: str) -> Generator:
    Path(old).rename(Path(new))
    logger.info('Renaming Job DONE')
    yield


def create_file(path_to_file: str) -> Generator:
    with open(path_to_file, 'w') as file:
        pass
    logger.info('A blank file was created at: <%s>. Job DONE',
                path_to_file)
    yield


def remove_file(path_to_file: str) -> Generator:
    Path(path_to_file).unlink()
    logger.info('The <%s> file has been deleted. Job COMPLETED', path_to_file)
    yield


def read_file(path_to_file: str) -> Generator:
    with open(path_to_file, "r") as file:
        content = file.read()
    logger.info('The contents of the %s file have been read. Job COMPLETE',
                path_to_file)
    yield


def write_file(path_to_file: str, content: str) -> Generator:
    with open(path_to_file, 'w') as file:
        file.write(content)
    logger.info('The %s file is saved with the content installed',
                path_to_file)
    yield


def adscript_file(path_to_file: str, additional_content: str) -> Generator:
    with open(path_to_file, 'a') as file:
        file.write(additional_content)
    logger.info('Additional content added to the file<%s>',
                path_to_file)
    yield


def get_data_from_url(url: str) -> Generator:
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.text
        data = data[:10]
        logger.info('The task received the following data: %s', data)
    else:
        logger.warning(
            'The job was unable to retrieve the data. Error code: %s',
            resp.status_code
        )
    yield
