import asyncio
from functools import wraps
import socket
import time
from typing import Any, Callable, Dict

from aiohttp import web
from aiohttp.web_app import Application
from aiohttp.web_response import Response
from aioprometheus import Histogram
from aioprometheus import Registry
from aioprometheus import render
from aioprometheus import Summary
from aioprometheus import timer

# REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")
REQUEST_TIME = Histogram("request_duration_seconds", "Time (in seconds) spent serving HTTP requests")


async def init_metrics(app: Application):
    app.registry = Registry()
    # app.events_counter = Counter("events", "Number of events.", const_labels={"host": socket.gethostname()})

    app.registry.register(REQUEST_TIME)


# def request_timer(labels: Dict[str, str] = None) -> Callable[..., Any]:
#     # TODO: Histogram observe with "method", "route", "status_code"
#     return timer(REQUEST_TIME, labels)

def request_timer(labels: Dict[str, str] = None) -> Callable[..., Any]:
    metric = REQUEST_TIME

    def track(func):
        """
        This function wraps the callable with metric incremeting logic.

        :param func: the callable to be monitored for exceptions.

        :returns: the return value from the decorated callable.
        """

        @wraps(func)
        async def async_func_wrapper(*args, **kwds):
            start_time = time.monotonic()
            status_code = None
            try:
                rv = func(*args, **kwds)
                if isinstance(rv, asyncio.Future) or asyncio.iscoroutine(rv):
                    rv = await rv

                    if isinstance(rv, Response):
                        status_code = rv.status

                    metric.observe(dict({'status_code': status_code or 200},
                                        **(labels or {})), time.monotonic() - start_time)
                    print(time.monotonic() - start_time)
            except Exception:
                metric.observe(dict({'status_code': status_code or '500'},
                                    **(labels or {})), time.monotonic() - start_time)
                raise
            return rv

        @wraps(func)
        def func_wrapper(*args, **kwds):
            start_time = time.monotonic()
            try:
                rv = func(*args, **kwds)
                metric.observe(dict({'status_code': '200'}, **(labels or {})), time.monotonic() - start_time)
            except Exception:
                metric.observe(dict({'status_code': '500'}, **(labels or {})), time.monotonic() - start_time)
                raise
            return rv

        if asyncio.iscoroutinefunction(func):
            return async_func_wrapper
        return func_wrapper

    return track


async def handle_metrics(request: web.Request):
    """
    Negotiate a response format by inspecting the ACCEPTS headers and selecting
    the most efficient format. Render metrics in the registry into the chosen
    format and return a response.
    """
    app = request.app['main_app']
    content, http_headers = render(app.registry, [request.headers.get("accept")])
    return Response(body=content, headers=http_headers)
