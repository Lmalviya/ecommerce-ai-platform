import requests
from .exceptions import DownloadError

class ImageDownloader:
    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()
        # Pretend to be a browser to avoid blocks
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) IndexerBot/1.0"
        })

    def download(self, url: str) -> bytes:
        try:
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            raise DownloadError(f"Network error downloading {url}: {str(e)}")
