from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
import logging


class GCSObjectStreamUpload(object):
    def __init__(self, client, bucket_name, blob_name, chunk_size=2560 * 8 * 1024):

        self._client = client
        self._bucket = self._client.bucket(bucket_name)
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b""
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(credentials=self._client._credentials)
        self._request = None  # type: requests.ResumableUpload

    def __enter__(self):
        try:
            self.start()
        except ValueError as e:
            logging.info("Error " + str(e))
            raise SystemExit
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        url = "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable".format(
            self._bucket.name
        )
        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size
        )
        self._request.initiate(
            transport=self._transport,
            content_type="application/octet-stream",
            stream=self,
            stream_final=False,
            metadata={"name": self._blob.name},
        )

    def stop(self):
        self._request.transmit_next_chunk(self._transport)

    def write(self, data):
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                self._request.transmit_next_chunk(self._transport)
            except common.InvalidResponse:
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size):
        to_read = min(chunk_size, self._buffer_size)
        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self):
        return self._read
