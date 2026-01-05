from urllib.parse import urljoin

def urljoin_wrapper(base_url, url):
    if url.startswith('http'):
        return url
    else:
        return urljoin(base_url, url)

