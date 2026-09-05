import json
import ssl

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from typing import Any, Dict, Optional

from .base_client import BaseClient
from exceptions.errors import APIHTTPError, APINetworkError


class HTTPClient(BaseClient):

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        ssl_context=None,
    ):

        if params:
            query_string = urlencode(params)
            separator = "&" if "?" in url else "?"
            url = url + separator + query_string

        data = None

        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")

        request = Request(
            url=url,
            data=data,
            method=method,
            headers=headers or {},
        )

        try:
            with urlopen(
                request,
                timeout=timeout,
                context=ssl_context,
            ) as response:

                response_body = response.read()

                try:
                    body = json.loads(
                        response_body.decode("utf-8")
                    )
                except json.JSONDecodeError:
                    body = response_body.decode(
                        "utf-8",
                        errors="replace"
                    )

                return {
                    "status_code": response.status,
                    "headers": dict(response.headers),
                    "body": body,
                }

        except HTTPError as error:

            error_body = error.read().decode(
                "utf-8",
                errors="replace"
            )

            try:
                error_body = json.loads(error_body)
            except json.JSONDecodeError:
                pass

            raise APIHTTPError(
                status_code=error.code,
                message=(
                    f"API returned HTTP {error.code}: "
                    f"{error.reason}"
                ),
                response_body=error_body,
            )

        except URLError as error:

            raise APINetworkError(
                f"Network error: {error.reason}"
            )