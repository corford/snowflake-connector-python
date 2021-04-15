#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import chain
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

from snowflake.connector.options import installed_pandas
from snowflake.connector.result_chunk import DownloadMetrics, ResultChunk
from snowflake.connector.telemetry import TelemetryField
from snowflake.connector.time_util import get_time_millis

if TYPE_CHECKING:  # pragma: no cover
    from snowflake.connector.cursor import SnowflakeCursor

if installed_pandas:
    from pyarrow import concat_tables

logger = getLogger(__name__)


class ResultSetInterface(ABC, Iterable[List[Any]]):
    def __init__(
        self,
        cursor: "SnowflakeCursor",
        result_chunks: List["ResultChunk"],
    ):
        """Initialize a ResultSet with a connection and a list of ResultChunks."""
        self.partitions = result_chunks
        self._cursor = cursor
        self._iter: Optional[Iterator[List[Any]]] = None

    def _report_metrics(self) -> None:
        """Report all metrics totalled up.

        This includes TIME_CONSUME_LAST_RESULT, TIME_DOWNLOADING_CHUNKS and
        TIME_PARSING_CHUNKS in that order.
        """
        time_consume_last_result = get_time_millis() - self._cursor._first_chunk_time
        self._cursor._log_telemetry_job_data(
            TelemetryField.TIME_CONSUME_LAST_RESULT, time_consume_last_result
        )
        metrics = self._get_metrics()
        if DownloadMetrics.download.value in metrics:
            self._cursor._log_telemetry_job_data(
                TelemetryField.TIME_DOWNLOADING_CHUNKS,
                metrics.get(DownloadMetrics.download.value),
            )
        if DownloadMetrics.parse.value in metrics:
            self._cursor._log_telemetry_job_data(
                TelemetryField.TIME_PARSING_CHUNKS,
                metrics.get(DownloadMetrics.parse.value),
            )

    def _fetch_arrow_batches(self):
        # TODO test for not Arrow Chunks here for these functions
        pass

    def _fetch_arrow_all(self):
        """Fetches a single Arrow Table."""
        tables = list(self._fetch_arrow_batches())
        if tables:
            return concat_tables(tables)
        else:
            return None

    def _fetch_pandas_batches(self, **kwargs):
        """Fetches Pandas dataframes in batch, where 'batch' refers to Snowflake Chunk. Thus, the batch size (the
        number of rows in dataframe) is optimized by Snowflake Python Connector."""
        for table in self._fetch_arrow_batches():
            yield table.to_pandas(**kwargs)

    def _fetch_pandas_all(self, **kwargs):
        """Fetches a single Pandas dataframe."""
        table = self._fetch_arrow_all()
        if table:
            return table.to_pandas(**kwargs)
        else:

            return pandas.DataFrame(columns=self._column_idx_to_name)

    def _pre_download(self) -> None:
        """Pre-download some chunks at creation time."""

    def _get_metrics(self) -> Dict[str, int]:
        """Sum up all the chunks' metrics and show them together."""
        overall_metrics: Dict[str, int] = {}
        for c in self.partitions:
            for n, v in c._metrics.items():
                overall_metrics[n] = overall_metrics.get(n, 0) + v
        return overall_metrics

    @abstractmethod
    def __iter__(self) -> Iterator[List[Any]]:
        """Returns a new iterator through all partitions."""
        raise NotImplementedError

    def __next__(self) -> List[Any]:
        try:
            return next(self._iter)
        except StopIteration:
            self._report_metrics()
            raise


class ResultSet(ResultSetInterface):
    """This class retrieves the results of a query with the historical strategy.

    It pre-downloads the first up to 4 ResultChunks (this doesn't include the 1st chunk
    as that is embedded in the response JSON from Snowflake).
    """

    def __iter__(self) -> Iterator[List[Any]]:
        """Set up a new iterator through all partitions with first 5 chunks ready."""
        with ThreadPoolExecutor(4) as pool:
            results = pool.map(iter, self.partitions[1:5])
        pre_downloaded_iters: List[Iterator[List[Any]]] = list(results)

        # TODO find a better way than keeping an internal iterator
        self._iter = chain(
            self.partitions[0],  # Embedded by Snowflake chunk
            *pre_downloaded_iters,  # Pre-downloaded chunks
            *self.partitions[5:],  # On-demand downloaded chunks
        )
        return self


class NoPrefetchResultSet(ResultSetInterface):
    """This class retrieves the results of a query with the on-demand strategy.

    It pre-downloads none of the chunks at creation time.
    """

    def __iter__(self) -> Iterator[List[Any]]:
        """Set up a new iterator through all partitions with first 5 chunks ready."""

        # TODO find a better way than keeping an internal iterator
        self._iter = chain.from_iterable(
            self.partitions,
        )
        return self
