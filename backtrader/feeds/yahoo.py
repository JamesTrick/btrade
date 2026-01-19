#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2023 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
import asyncio
import collections
from datetime import date, datetime
import io
import itertools
from typing import Iterable

import httpx
import pandas as pd

from .base import Bar, BarSeries

import backtrader as bt
from .. import feed
from ..utils import date2num


class YahooFinanceCSVData(feed.CSVDataBase):
    '''
    Parses pre-downloaded Yahoo CSV Data Feeds (or locally generated if they
    comply to the Yahoo format)

    Specific parameters:

      - ``dataname``: The filename to parse or a file-like object

      - ``reverse`` (default: ``False``)

        It is assumed that locally stored files have already been reversed
        during the download process

      - ``adjclose`` (default: ``True``)

        Whether to use the dividend/split adjusted close and adjust all
        values according to it.

      - ``adjvolume`` (default: ``True``)

        Do also adjust ``volume`` if ``adjclose`` is also ``True``

      - ``round`` (default: ``True``)

        Whether to round the values to a specific number of decimals after
        having adjusted the close

      - ``roundvolume`` (default: ``0``)

        Round the resulting volume to the given number of decimals after having
        adjusted it

      - ``decimals`` (default: ``2``)

        Number of decimals to round to

      - ``swapcloses`` (default: ``False``)

        [2018-11-16] It would seem that the order of *close* and *adjusted
        close* is now fixed. The parameter is retained, in case the need to
        swap the columns again arose.

    '''
    lines = ('adjclose',)

    params = (
        ('reverse', False),
        ('adjclose', True),
        ('adjvolume', True),
        ('round', True),
        ('decimals', 2),
        ('roundvolume', False),
        ('swapcloses', False),
    )

    def start(self):
        super(YahooFinanceCSVData, self).start()

        if not self.params.reverse:
            return

        # Yahoo sends data in reverse order and the file is still unreversed
        dq = collections.deque()
        for line in self.f:
            dq.appendleft(line)

        f = io.StringIO(newline=None)
        f.writelines(dq)
        f.seek(0)
        self.f.close()
        self.f = f

    def _loadline(self, linetokens):
        while True:
            nullseen = False
            for tok in linetokens[1:]:
                if tok == 'null':
                    nullseen = True
                    linetokens = self._getnextline()  # refetch tokens
                    if not linetokens:
                        return False  # cannot fetch, go away

                    # out of for to carry on wiwth while True logic
                    break

            if not nullseen:
                break  # can proceed

        i = itertools.count(0)

        dttxt = linetokens[next(i)]
        dt = date(int(dttxt[0:4]), int(dttxt[5:7]), int(dttxt[8:10]))
        dtnum = date2num(datetime.combine(dt, self.p.sessionend))

        self.lines.datetime[0] = dtnum
        o = float(linetokens[next(i)])
        h = float(linetokens[next(i)])
        l = float(linetokens[next(i)])
        c = float(linetokens[next(i)])
        self.lines.openinterest[0] = 0.0

        # 2018-11-16 ... Adjusted Close seems to always be delivered after
        # the close and before the volume columns
        adjustedclose = float(linetokens[next(i)])
        try:
            v = float(linetokens[next(i)])
        except:  # cover the case in which volume is "null"
            v = 0.0

        if self.p.swapcloses:  # swap closing prices if requested
            c, adjustedclose = adjustedclose, c

        adjfactor = c / adjustedclose

        # in v7 "adjusted prices" seem to be given, scale back for non adj
        if self.params.adjclose:
            o /= adjfactor
            h /= adjfactor
            l /= adjfactor
            c = adjustedclose
            # If the price goes down, volume must go up and viceversa
            if self.p.adjvolume:
                v *= adjfactor

        if self.p.round:
            decimals = self.p.decimals
            o = round(o, decimals)
            h = round(h, decimals)
            l = round(l, decimals)
            c = round(c, decimals)

        v = round(v, self.p.roundvolume)

        self.lines.open[0] = o
        self.lines.high[0] = h
        self.lines.low[0] = l
        self.lines.close[0] = c
        self.lines.volume[0] = v
        self.lines.adjclose[0] = adjustedclose

        return True



class YahooFinanceCSV(feed.CSVFeedBase):
    DataCls = YahooFinanceCSVData


class YahooFetcher:
    """Fetch to collect data from YahooFinance. Some errors you may receive:

    * Error 429: This is where the rate limit is received. Commonly used when no Headers are used. Try changing the
    headers of the fetcher to overcome this.
    """
    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    }

    def __init__(self, client: httpx.AsyncClient = None):
        self.client = client or httpx.AsyncClient(headers=self.HEADERS, follow_redirects=True)

    async def fetch_data(self, symbol: str, start: datetime, end: datetime, timeframe: str = "1d") -> BarSeries:
        """

        Args:
            symbol: String symbol of ticker you wish to fetch.
            start: Start date time of ticker you wish to fetch.
            end: End date time of ticker you wish to fetch.
            timeframe: Time period you wish to fetch.

        Returns:

        """
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": timeframe,
            "events": "history"
        }

        response = await self.client.get(f"{self.BASE_URL}{symbol}", params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("chart", {}).get("error"):
            raise ValueError(f"Yahoo API Error: {data['chart']['error']}")

        result = data["chart"]["result"][0]
        if "timestamp" not in result:
            print(f"WARNING: No trading data found for {symbol} between {start} and {end}.")
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        timestamps = result["timestamp"]
        ohlcv = result["indicators"]["quote"][0]

        bars = []

        for i, ts in enumerate(timestamps):
            # Skip bars with None values
            if any(ohlcv[k][i] is None for k in ["open", "high", "low", "close"]):
                continue

            bars.append(Bar(
                timestamp=datetime.fromtimestamp(ts),
                open=ohlcv["open"][i],
                high=ohlcv["high"][i],
                low=ohlcv["low"][i],
                close=ohlcv["close"][i],
                volume=ohlcv["volume"][i] or 0.0
            ))

        return BarSeries(bars=bars, symbol=symbol, timeframe=timeframe)



class YahooFinanceData(bt.feed.DataBase):
    '''YahooFinanceData collects data from the YahooFinance API.

    Specific parameters (or specific meaning):

      - ``dataname``

        The ticker to download ('NVDA' for Nividia's stock quotes)

      - ``proxies``

        A dict indicating which proxy to go through for the download as in
        {'http': 'http://myproxy.com'} or {'http': 'http://127.0.0.1:8080'}

      - ``period``

        The timeframe to download data in. Pass 'w' for weekly and 'm' for
        monthly.

      - ``reverse``

        [2018-11-16] The latest incarnation of Yahoo online downloads returns
        the data in the proper order. The default value of ``reverse`` for the
        online download is therefore set to ``False``

      - ``adjclose``

        Whether to use the dividend/split adjusted close and adjust all values
        according to it.

      - ``urlhist``

        The url of the historical quotes in Yahoo Finance used to gather a
        ``crumb`` authorization cookie for the download

      - ``urldown``

        The url of the actual download server

      - ``retries``

        Number of times (each) to try to get a ``crumb`` cookie and download
        the data

      '''

    params = (
        ('fetcher', None),
        ('fromdate', None),
        ('todate', None),
        ('timeframe', bt.TimeFrame.Days),
    )

    def __init__(self):
        super().__init__()
        self._df: pd.DataFrame = None
        self._idx: int = -1

    def start(self):
        if not self.p.fromdate or not self.p.todate:
            raise ValueError("YahooFinanceData requires 'fromdate' and 'todate'")

        fetcher = self.p.fetcher or YahooFetcher()

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        self._bars = loop.run_until_complete(
            fetcher.fetch_data(
                symbol=self.p.dataname,
                start=self.p.fromdate,
                end=self.p.todate,
                timeframe='1d'  # Map as needed
            )
        )
        self._idx = -1

    def _load(self):
        self._idx += 1
        if self._idx >= len(self._bars):
            return False

        bar = self._bars.bars[self._idx]
        bar_dict = bar.to_lines()

        # Cleaner assignment
        for key, value in bar_dict.items():
            getattr(self.lines, key)[0] = value

        return True


class YahooFinance(feed.CSVFeedBase):
    DataCls = YahooFinanceData

    params = DataCls.params._gettuple()
