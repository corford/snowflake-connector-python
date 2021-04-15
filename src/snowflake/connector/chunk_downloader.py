#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import json
import time
from abc import ABC
from collections import namedtuple
from concurrent.futures.thread import ThreadPoolExecutor
from gzip import GzipFile
from logging import getLogger
from threading import Condition, Lock
from typing import List

from snowflake.connector.gzip_decoder import decompress_raw_data

from .arrow_context import ArrowConverterContext
from .errorcode import ER_CHUNK_DOWNLOAD_FAILED
from .errors import Error, OperationalError
from .time_util import DecorrelateJitterBackoff, get_time_millis

DEFAULT_REQUEST_TIMEOUT = 7

DEFAULT_CLIENT_PREFETCH_THREADS = 4
MAX_CLIENT_PREFETCH_THREADS = 10

MAX_RETRY_DOWNLOAD = 10
MAX_WAIT = 360
WAIT_TIME_IN_SECONDS = 10

SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm"
SSE_C_KEY = "x-amz-server-side-encryption-customer-key"
SSE_C_AES = "AES256"

SnowflakeChunk = namedtuple(
    "SnowflakeChunk",
    [
        "url",  # S3 bucket URL to download the chunk
        "row_count",  # number of rows in the chunk
        "result_data",  # pointer to the generator of the chunk
        "ready",  # True if ready to consume or False
    ],
)

logger = getLogger(__name__)


class SnowflakeChunkDownloader(object):
    """Large Result set chunk downloader class."""

    def __init__(
        self,
        chunks,
        connection,
        cursor,
        qrmk,
        chunk_headers,
        query_result_format: str = "JSON",
        prefetch_threads=DEFAULT_CLIENT_PREFETCH_THREADS,
    ):
        self._query_result_format = query_result_format

        self._downloader_error = None

        self._connection = connection
        self._cursor = cursor
        self._qrmk = qrmk
        self._chunk_headers = chunk_headers

        self._chunk_size = len(chunks)
        self._chunks: List["SnowflakeChunk"] = list()
        self._chunk_cond = Condition()

        self._effective_threads = min(prefetch_threads, self._chunk_size)
        if self._effective_threads < 1:
            self._effective_threads = 1
        for idx, chunk in enumerate(chunks):
            logger.debug(f"queued chunk {idx}: rowCount={chunk['rowCount']}")
            self._chunks[idx] = SnowflakeChunk(
                url=chunk["url"],
                result_data=None,
                ready=False,
                row_count=int(chunk["rowCount"]),
            )

        logger.debug(
            f"prefetch threads: {prefetch_threads}, number of chunks: {self._chunk_size}, "
            f"effective threads: {self._effective_threads}"
        )

        self._pool = ThreadPoolExecutor(self._effective_threads)

        self._downloading_chunks_lock = Lock()
        self._total_millis_downloading_chunks = 0
        self._total_millis_parsing_chunks = 0

        self._next_chunk_to_consume = 0
        for idx in range(self._effective_threads):
            self._pool.submit(self._download_chunk, idx)
        self._next_chunk_to_download = self._effective_threads

    def _download_chunk(self, idx):
        """Downloads a chunk asynchronously."""
        logger.debug(f"downloading chunk {idx+1}/{self._chunk_size}")
        headers = {}
        if self._chunk_headers is not None:
            headers = self._chunk_headers
            logger.debug("use chunk headers from result")
        elif self._qrmk is not None:
            headers[SSE_C_ALGORITHM] = SSE_C_AES
            headers[SSE_C_KEY] = self._qrmk

        last_error = None
        backoff = DecorrelateJitterBackoff(1, 16)
        sleep_timer = 1
        for retry in range(10):
            try:
                logger.debug(
                    f"started getting the result set {idx+1}: {self._chunks[idx].url}"
                )
                result_data = self._fetch_chunk(self._chunks[idx].url, headers)
                logger.debug(
                    f"finished getting the result set {idx+1}: {self._chunks[idx].url}"
                )

                if isinstance(result_data, ResultIterWithTimings):
                    metrics = result_data.get_timings()
                    with self._downloading_chunks_lock:
                        self._total_millis_downloading_chunks += metrics[
                            ResultIterWithTimings.DOWNLOAD
                        ]
                        self._total_millis_parsing_chunks += metrics[
                            ResultIterWithTimings.PARSE
                        ]

                with self._chunk_cond:
                    self._chunks[idx] = self._chunks[idx]._replace(
                        result_data=result_data, ready=True
                    )
                    self._chunk_cond.notify_all()
                    logger.debug(
                        f"added chunk {idx+1}/{self._chunk_size} to a chunk list."
                    )
                break
            except Exception as e:
                last_error = e
                sleep_timer = backoff.next_sleep(1, sleep_timer)
                logger.exception(
                    f"Failed to fetch the large result set chunk {idx+1}/{self._chunk_size} "
                    f"for the {retry+1} th time, backing off for {sleep_timer} s"
                )
                time.sleep(sleep_timer)
        else:
            self._downloader_error = last_error

    def next_chunk(self):
        """Gets the next chunk if ready."""
        logger.debug(
            f"next_chunk_to_consume={self._next_chunk_to_consume+1}, "
            f"next_chunk_to_download={self._next_chunk_to_download+1}, "
            f"total_chunks={self._chunk_size}"
        )
        if self._next_chunk_to_consume > 0:
            # clean up the previously fetched data
            n = self._next_chunk_to_consume - 1
            self._chunks[n] = self._chunks[n]._replace(result_data=None, ready=False)

            if self._next_chunk_to_download < self._chunk_size:
                self._pool.submit(self._download_chunk, self._next_chunk_to_download)
                self._next_chunk_to_download += 1

        if self._downloader_error is not None:
            raise self._downloader_error

        for attempt in range(MAX_RETRY_DOWNLOAD):
            logger.debug(
                f"waiting for chunk {self._next_chunk_to_consume+1}/{self._chunk_size} "
                f"in {attempt+1}/{MAX_RETRY_DOWNLOAD} download attempt"
            )
            done = False
            for wait_counter in range(MAX_WAIT):
                with self._chunk_cond:
                    if self._downloader_error:
                        raise self._downloader_error
                    if self._chunks[self._next_chunk_to_consume].ready:
                        done = True
                        break
                    logger.debug(
                        f"chunk {self._next_chunk_to_consume+1}/{self._chunk_size} is NOT ready to consume "
                        f"in {(wait_counter + 1) * WAIT_TIME_IN_SECONDS}/{MAX_WAIT * WAIT_TIME_IN_SECONDS}(s)"
                    )
                    self._chunk_cond.wait(WAIT_TIME_IN_SECONDS)
            else:
                logger.debug(
                    f"chunk {self._next_chunk_to_consume+1}/{self._chunk_size} is still NOT ready. Restarting chunk "
                    "downloader threads"
                )
                self._pool.shutdown(wait=False)  # terminate the thread pool
                self._pool = ThreadPoolExecutor(self._effective_threads)
                for idx0 in range(self._effective_threads):
                    idx = idx0 + self._next_chunk_to_consume
                    self._pool.submit(self._download_chunk, idx)
            if done:
                break
        else:
            Error.errorhandler_wrapper(
                self._connection,
                self._cursor,
                OperationalError,
                {
                    "msg": "The result set chunk download fails or hang for "
                    "unknown reason.",
                    "errno": ER_CHUNK_DOWNLOAD_FAILED,
                },
            )
        logger.debug(
            f"chunk {self._next_chunk_to_consume+1}/{self._chunk_size} is ready to consume"
        )

        ret = self._chunks[self._next_chunk_to_consume]
        self._next_chunk_to_consume += 1
        return ret

    def terminate(self):
        """Terminates downloading the chunks."""
        if hasattr(self, "_pool") and self._pool is not None:
            self._pool.shutdown()
            self._pool = None

    def __del__(self):
        try:
            self.terminate()
        except Exception:
            # ignore all errors in the destructor
            pass

    def _fetch_chunk(self, url, headers):
        """Fetch the chunk from S3."""
        handler = (
            JsonBinaryHandler(is_raw_binary_iterator=True)
            if self._query_result_format == "json"
            else ArrowBinaryHandler(self._cursor, self._connection)
        )

        return self._connection.rest.fetch(
            "get",
            url,
            headers,
            timeout=DEFAULT_REQUEST_TIMEOUT,
            is_raw_binary=True,
            binary_data_handler=handler,
        )


class ResultIterWithTimings:
    DOWNLOAD = "download"
    PARSE = "parse"

    def __init__(self, it, timings):
        self._it = it
        self._timings = timings

    def __next__(self):
        return next(self._it)

    def next(self):
        return self.__next__()

    def get_timings(self):
        return self._timings


class RawBinaryDataHandler(ABC):
    """Abstract class being passed to network.py to handle raw binary data."""

    def to_iterator(self, raw_data_fd, download_time):
        pass


# TODO Deprecate
class JsonBinaryHandler(RawBinaryDataHandler):
    """Convert result chunk in json format into interator."""

    def __init__(self, is_raw_binary_iterator):
        self._is_raw_binary_iterator = is_raw_binary_iterator

    def to_iterator(self, raw_data_fd, download_time):
        parse_start_time = get_time_millis()
        raw_data = decompress_raw_data(raw_data_fd, add_bracket=True).decode(
            "utf-8", "replace"
        )
        if not self._is_raw_binary_iterator:
            ret = json.loads(raw_data)
        ret = iter(json.loads(raw_data))

        parse_end_time = get_time_millis()

        timing_metrics = {
            ResultIterWithTimings.DOWNLOAD: download_time,
            ResultIterWithTimings.PARSE: parse_end_time - parse_start_time,
        }

        return ResultIterWithTimings(ret, timing_metrics)


class ArrowBinaryHandler(RawBinaryDataHandler):
    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._arrow_context = ArrowConverterContext(connection._session_parameters)

    """
    Handler to consume data as arrow stream
    """

    def to_iterator(self, raw_data_fd, download_time):
        from .arrow_iterator import PyArrowIterator

        gzip_decoder = GzipFile(fileobj=raw_data_fd, mode="r")
        return PyArrowIterator(
            self._cursor,
            gzip_decoder,
            self._arrow_context,
            self._cursor._use_dict_result,
            self._cursor.connection._numpy,
        )
