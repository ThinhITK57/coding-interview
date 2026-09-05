from urllib.request import ProxyHandler, build_opener


class ProxyConfig:

    def __init__(
        self,
        http_proxy=None,
        https_proxy=None,
    ):
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy


def build_proxy_opener(proxy_config):

    proxies = {}

    if proxy_config.http_proxy:
        proxies["http"] = proxy_config.http_proxy

    if proxy_config.https_proxy:
        proxies["https"] = proxy_config.https_proxy

    proxy_handler = ProxyHandler(proxies)

    return build_opener(proxy_handler)