from aiohttp.web_request import Request
from aiozipkin import HTTP_METHOD
from aiozipkin import HTTP_PATH
from aiozipkin import HTTP_ROUTE
from aiozipkin import SERVER
from aiozipkin import SpanAbc
from aiozipkin.aiohttp_helpers import _set_remote_endpoint


def _set_span_properties(span: SpanAbc, request: Request) -> None:
    span_name = f"{request.method.upper()} {request.path}"
    span.name(span_name)
    span.kind(SERVER)
    span.tag(HTTP_PATH, request.path)
    span.tag(HTTP_METHOD, request.method.upper())

    resource = request.match_info.route.resource
    if resource is not None:
        route = resource.canonical
        span.tag(HTTP_ROUTE, route)

    _set_remote_endpoint(span, request)
