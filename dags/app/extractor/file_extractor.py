import json
import queue
from multiprocessing import Process, Queue, cpu_count
from typing import Dict, Generator
from zipfile import ZipFile, is_zipfile

from app.extractor.validators import CompanyDir

class FileReader:
    """
    """

    queue_in = Queue()
    queue_out = Queue()
    # logger = get_logger(__name__)

    @classmethod
    def read_json_by_path(
            cls, path: str) -> Generator[Dict[str, str], None, None]:
        with open(path) as f:
            for item in json.load(f):
                yield item

    @classmethod
    def read_json_by_bytes(
            cls, payload: bytes) -> Generator[Dict[str, str], None, None]:
        for item in json.loads(payload):
            yield item

    @classmethod
    def read_zip(cls, path: str) -> Generator[Dict[str, str], None, None]:
        cls.logger.info('Reading started')

        if not is_zipfile(path):
            cls.logger.error(f'File is not a zip file: {path}')
            return

        with ZipFile(path) as zf:
            [cls.queue_in.put(filename) for filename in zf.namelist()]

        workers = [Process(
            target=cls.extractor,
            args=(path,),
            daemon=True) for __ in range(cpu_count())
            ]
        [worker.start() for worker in workers]

        while any([worker.is_alive() for worker in workers]):
            try:
                while True:
                    yield cls.queue_out.get(True, 0.1)

            except queue.Empty:
                continue

        cls.logger.info('Reading ended')

    @classmethod
    def extractor(cls, path: str) -> None:
        cls.logger.info('Worker started')
        with ZipFile(path) as zf:
            while not cls.queue_in.empty():
                filename = cls.queue_in.get()
                with zf.open(filename) as f:
                    for item in cls.read_json_by_bytes(f.read()):
                        if (parsed_item := cls.parse_item(item)):
                            cls.queue_out.put(parsed_item)

        cls.logger.info('Worker stopped')

    @classmethod
    def parse_item(cls, item: Dict[str, str]) -> Dict[str, str]:
        return item
                
                
class CustomReader(FileReader):
    @classmethod
    def parse_item(cls, item: Dict[str, str]) -> Dict[str, str] | None:
        okved = item.get(
            'data', {},
            ).get('СвОКВЭД', {}).get('СвОКВЭДОсн', {}).get('КодОКВЭД', '')

        if okved.split('.')[0] == '61':
            item['okved'] = okved
            return CompanyDir(**item).dict()