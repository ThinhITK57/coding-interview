import ssl

from config.loader import ConfigLoader
from config.validator import ConfigValidator

from auth.token_auth import TokenAuth
from client.http_client import HTTPClient


def main():

    loader = ConfigLoader()
    config = loader.load("config.json")

    validator = ConfigValidator()
    validator.validate(config)

    endpoint = config.api.endpoints[0]

    # -------------------------
    # Authentication
    # -------------------------
    headers = dict(endpoint.headers)

    if endpoint.auth:
        auth = TokenAuth(endpoint.auth)
        headers.update(auth.get_headers())

    # -------------------------
    # SSL
    # -------------------------
    ssl_context = ssl._create_unverified_context()

    # -------------------------
    # HTTP Client
    # -------------------------
    client = HTTPClient()

    print("================================")
    print("HTTP CLIENT TEST")
    print("================================")

    print("URL:", config.api.base_url + endpoint.path)
    print("Method:", endpoint.method.value)

    try:

        response = client.request(
            method=endpoint.method.value,
            url=config.api.base_url + endpoint.path,
            headers=headers,
            params=endpoint.params,
            json_body=endpoint.body,
            timeout=endpoint.timeout_seconds,
            ssl_context=ssl_context,
        )

        print()
        print("STATUS:", response["status_code"])

        print()
        print("BODY:")
        print(response["body"])

    except Exception as error:

        print()
        print("ERROR TYPE:", type(error).__name__)
        print("ERROR:", error)


if __name__ == "__main__":
    main()