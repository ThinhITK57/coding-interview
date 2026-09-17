import requests
import json
import time

def safe_request(url, headers=None, params=None, max_retries=5, timeout=150,proxies=None):
    resp = None
    for attempt in range(max_retries):
        try:
            print("Request to ", url , params)
            resp = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            print("\n⚠️ Timeout khi gọi {}. Thử lại {}/{}...".format(url, attempt + 1, max_retries))
            if attempt < max_retries - 1:
                time.sleep(1)
        except requests.exceptions.ProxyError as e:
            print("\n❌ ProxyError khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return resp
        except requests.exceptions.RequestException as e:
            print("\n❌ Lỗi request khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return resp
    return None


def safe_put_request(url, headers=None, params=None, max_retries=5, timeout=150,proxies=None, files=None,data=None, json_data=None):
    resp = None
    for attempt in range(max_retries):
        try:
            print("Request to ", url , params)
            resp = requests.put(url, headers=headers, files=files, data=data, json=json_data, timeout=timeout, proxies=proxies)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            print("\n⚠️ Timeout khi gọi {}. Thử lại {}/{}...".format(url, attempt + 1, max_retries))
            if attempt < max_retries - 1:
                time.sleep(5)
        except requests.exceptions.ProxyError as e:
            print("\n❌ ProxyError khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return resp
        except requests.exceptions.RequestException as e:
            print("\n❌ Lỗi request khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return resp
    return None


def safe_post_request(url, headers=None, params=None, max_retries=5, timeout=150,proxies=None, files=None,data=None, json_data=None):
    resp = None
    for attempt in range(max_retries):
        try:
            print("Request to ", url , params)
            resp = requests.post(url, headers=headers, files=files, data=data, json=json_data, timeout=timeout, proxies=proxies)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            print("\n⚠️ Timeout khi gọi {}. Thử lại {}/{}...".format(url, attempt + 1, max_retries))
            if attempt < max_retries - 1:
                time.sleep(5)
        except requests.exceptions.ProxyError as e:
            print("\n❌ ProxyError khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return resp
        except requests.exceptions.RequestException as e:
            print("\n❌ Lỗi request khi gọi {}: {}".format(url, e))
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return resp
    return None
