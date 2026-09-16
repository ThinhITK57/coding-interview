import os

def build_request_proxies():
    proxies = {}
    
    CRAWLER_HTTP_PROXY = os.getenv("CRAWLER_HTTP_PROXY")
    CRAWLER_HTTPS_PROXY = os.getenv("CRAWLER_HTTPS_PROXY")

    if CRAWLER_HTTP_PROXY:
        proxy = CRAWLER_HTTP_PROXY

        if not proxy.startswith(("http://", "https://")):
            proxy = f"http://{proxy}"

        proxies["http"] = proxy

    if CRAWLER_HTTPS_PROXY:
        proxy = CRAWLER_HTTPS_PROXY

        if not proxy.startswith(("http://", "https://")):
            proxy = f"http://{proxy}"

        proxies["https"] = proxy

    return proxies if proxies else None
