import time
from typing import Dict, Generator

import httpx

from app.extractor.validators import ResponseModel, VacancyModel
from app.settings import API_URL, RETRY, SESSION_HEADERS, TIMEOUT


def extract_vacancy(_id: str, client: httpx.Client) -> Dict[str, str]:
    retry = RETRY
    result = None
    while retry:
        response = client.get(API_URL + _id)

        if response.is_error:
            time.sleep(TIMEOUT)
            retry -= 1
            continue

        else:
            result = response.json()
            break

    return result


def api_extractor(query: str) -> Generator[Dict[str, str], None, None]:

    with httpx.Client(headers=SESSION_HEADERS) as client:
        page = 0

        while True:
            resp = client.get(API_URL, params={
                'text': query,
                'page': page,
                })

            rm = ResponseModel(**resp.json())

            if not rm.items:
                break

            for i in rm.items:
                result = extract_vacancy(i.id, client)

                if result:
                    yield VacancyModel(**result).dict()

                page += 1
