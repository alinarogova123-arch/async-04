import aiohttp
import asyncio
import pymorphy2
import text_tools
import aiofiles
import sys
import time
import pytest
import logging
from functools import partial
from aiohttp import web
from contextlib import contextmanager
from enum import Enum
from asyncio import timeout
from anyio import create_task_group
from adapters.inosmi_ru import sanitize
from adapters.exceptions import ArticleNotFound


FETCH_TIMEOUT = 1

PROCESSING_TIMEOUT = 3


class MyLogsHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        print(log_entry)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(MyLogsHandler())


class ProcessingStatus(Enum):
    OK = 'OK'
    FETCH_ERROR = 'FETCH_ERROR'
    PARSING_ERROR = "PARSING_ERROR"
    FETCH_TIMEOUT = "FETCH_TIMEOUT"
    PROCESSING_TIMEOUT = "PROCESSING_TIMEOUT"


@contextmanager
def timer():
    start_time = time.monotonic()
    cm = {}
    try:
        yield cm
    finally:
        end_time = time.monotonic()
        duration = round(end_time - start_time, 2)
        cm["duration"] = duration


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientResponseError:
        raise aiohttp.ClientError


async def process_article(news, session, url, morph, charged_words, fetch_timeout, processing_timeout):
    with timer() as cm:
        try:
            async with timeout(fetch_timeout) as tm:
                html = await fetch(session, url)
            
            text = sanitize(html, True)
            
            async with timeout(processing_timeout):
                words = await text_tools.split_by_words(morph, text)
            
            rate = text_tools.calculate_jaundice_rate(words, charged_words)
            status = ProcessingStatus.OK.value
        
        except aiohttp.ClientError:
            words, rate = None, None
            status = ProcessingStatus.FETCH_ERROR.value
        except ArticleNotFound:
            words, rate = None, None
            status = ProcessingStatus.PARSING_ERROR.value
        except TimeoutError:
            words, rate = None, None
            if tm.expired():
                status = ProcessingStatus.FETCH_TIMEOUT.value
            else:
                status = ProcessingStatus.PROCESSING_TIMEOUT.value
   
    duration = cm["duration"]

    news_info = {
        "url": url,
        "status": status,
        "rate": rate,
        "words_count": len(words) if words else None,
    }
    news.append(news_info)


@pytest.mark.asyncio
async def test_process_article():
    charged_words = []
    morph = pymorphy2.MorphAnalyzer()
    news = []

    url = "https://inosmi.ru/fetch/error.html"
    async with aiohttp.ClientSession() as session:
        await process_article(news, session, url, morph, charged_words, FETCH_TIMEOUT, PROCESSING_TIMEOUT)
    assert news.pop(0).get("status") == "FETCH_ERROR"

    url = "https://docs.aiohttp.org/en/stable/"
    async with aiohttp.ClientSession() as session:
        await process_article(news, session, url, morph, charged_words, FETCH_TIMEOUT, PROCESSING_TIMEOUT)
    assert news.pop(0).get("status") == "PARSING_ERROR"

    url = "https://inosmi.ru/20261020/frantsiya-280733515.html"
    async with aiohttp.ClientSession() as session:
        await process_article(news, session, url, morph, charged_words, 0.1, PROCESSING_TIMEOUT)
    assert news.pop(0).get("status") == "FETCH_TIMEOUT"

    url = "https://inosmi.ru/20261020/frantsiya-280733515.html"
    async with aiohttp.ClientSession() as session:
        await process_article(news, session, url, morph, charged_words, FETCH_TIMEOUT, 0.1)
    assert news.pop(0).get("status") == "PROCESSING_TIMEOUT"


async def handle(morph, request):
    urls = dict(request.query).get("urls")
    urls = urls.split(",")
    news = []
    
    if len(urls) > 10:
        error = {
            "error": "too many urls in request, should be 10 or less",
        }
        return web.json_response(error, status=400)

    async with aiofiles.open("charged_dict/negative_words.txt", "r", encoding="utf-8") as file:
        negative_words = await file.read()
    async with aiofiles.open("charged_dict/positive_words.txt", "r", encoding="utf-8") as file:
        positive_words = await file.read()
    charged_words = negative_words.split("\n") + positive_words.split("\n")

    async with aiohttp.ClientSession() as session:
        async with create_task_group() as tg:
            for url in urls:
                tg.start_soon(
                    process_article,
                    news,
                    session,
                    url,
                    morph,
                    charged_words,
                    FETCH_TIMEOUT,
                    PROCESSING_TIMEOUT
                )

    return web.json_response(news)


if __name__ == '__main__':
    morph = pymorphy2.MorphAnalyzer()
    morph_handle = partial(handle, morph)
    app = web.Application()
    app.add_routes([
        web.get('/', morph_handle),
    ])
    web.run_app(app)
