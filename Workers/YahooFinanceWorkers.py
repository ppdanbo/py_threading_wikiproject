"""Yahoo Finance price workers and scheduler.

This module contains two convenience classes used by the demo scheduler:

- ``YahooFinancePriceScheduler``: a ``threading.Thread`` subclass that reads
    symbols from an input queue and uses ``YahooFinacePriceWorker`` to fetch
    prices for each symbol.
- ``YahooFinacePriceWorker``: a small helper that requests the Yahoo Finance
    quote page for a single ticker symbol and attempts to parse the current
    price using an XPath expression.

Notes
-----
- This file is intentionally kept small and focused on illustrating a simple
    threaded scheduler + worker pattern used in the tutorial. It intentionally
    mirrors the original behavior and does not change runtime logic.
"""

import threading
import datetime
import requests
from lxml import html
import time
import random
from datetime import datetime


class YahooFinancePriceScheduler(threading.Thread):
    """Threaded scheduler that consumes symbols from a queue and fetches prices.

    The scheduler expects an ``input_queue`` with a blocking ``get`` method
    supporting a ``timeout`` parameter. Each value taken from the queue is
    treated as a ticker symbol and passed to a ``YahooFinacePriceWorker`` to
    fetch the latest price. When the value ``'DONE'`` is received the thread
    will exit.

    Note: the original implementation starts the thread inside ``__init__``
    (``self.start()``). That behaviour is preserved here but callers may prefer
    to start the thread explicitly after construction.
    """

    def __init__(self, input_queue, output_queues, **kwargs):
        """Initialize the scheduler and start the thread.

        Parameters
        ----------
        input_queue : queue-like
            A queue providing symbols to process. The queue must implement
            ``get(timeout=...)``.
        """
        super(YahooFinancePriceScheduler, self).__init__()
        self._input_queue = input_queue
        temp_queue = output_queues
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queues = temp_queue
        self.start()

    def run(self):
        """Main scheduler loop.

        Repeatedly pulls values from ``self._input_queue``. For each symbol a
        ``YahooFinacePriceWorker`` is created and used to obtain the price.

        The loop exits if the queue returns the sentinel value ``'DONE'`` or if
        an exception occurs when reading from the queue.
        """
        while True:
            try:
                val = self._input_queue.get()
            except Exception as e:
                print(f"Yahoo scheduler queue schedule has exception as {e}, stopping")
                break            
            if val == "DONE":                
                # if self._output_queues is not None:
                #     for output_queue in self._output_queues:
                #             output_queue.put(("DONE", None, None))                
                break
            
            yahooFinacePriceWorker = YahooFinacePriceWorker(symbol=val)
            price = yahooFinacePriceWorker.get_price_for_symbol()
            # print(f"Yahoo scheduler queue got price {price} for symbol {val}")

            if self._output_queues is not None and price is not None:
                ingest_date = datetime.utcnow()               
                output_vals = ( val, price, ingest_date) 
                for output_queue in self._output_queues:            
                    output_queue.put(output_vals)
                # print(f"Yahoo scheduler queue put price {price} for symbol {val} into output queues")
            time.sleep(random.random())  # to avoid hitting Yahoo too fast


class YahooFinacePriceWorker:
    """Fetch the current price for a single Yahoo Finance ticker symbol.

    The class constructs the appropriate quote page URL for ``symbol`` and
    attempts to parse the price using a hard-coded XPath. The method
    ``get_price_for_symbol`` returns a floating point price on success or
    ``None`` on failure.
    """

    def __init__(self, symbol):
        """Create a worker for the provided ticker symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol (for example, 'AAPL' or 'MSFT').
        """
        self._symbol = symbol
        base_url = "https://finance.yahoo.com/quote/"
        self._url = f"{base_url}{self._symbol}"        

    def get_price_for_symbol(self):
        """Request the Yahoo quote page and parse the current price.

        Returns
        -------
        float or None
            The parsed price value as a float if successful, otherwise ``None``.

        Notes
        -----
        - The implementation uses a fixed XPath that may break if Yahoo
          changes their page layout.
        - Network errors, non-200 responses, or missing elements result in a
          ``None`` return value and the exception (if any) is printed.
        """
        try:
            r = requests.get(self._url)
            # print(f"get_price_for_symbol: status code: {r.status_code} for url: {self._url}")
            if r.status_code != 200:
                return
            page_contents = html.fromstring(r.text)
            path_id = '//*[@id="main-content-wrapper"]/section[6]/div/div/div/section[1]/div/div[2]/div'
            raw_price = page_contents.xpath(path_id)[0].text
            price = float(raw_price.replace(",", ""))
            # print(f"price for {self._symbol} is {price} USD as of {datetime.datetime.utcnow()} UTC ")
            return price
        except Exception as e:
            print(f"    Exception getting price for {self._symbol}: {e} via url: {self._url}")
            return
