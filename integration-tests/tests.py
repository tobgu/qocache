import json
import random
import unittest
from qclient import QClient
import lz4.frame
import lz4.block

client = QClient(node_list=('http://localhost:8888',))


def generate_csv(byte_size):
    header = b'abc,def,ghi,jkl,mno\r\n'

    body = []
    l = len(header)
    row_count = 0
    while True:
        row_count += 1
        number = round(random.uniform(-1000, 1000), 2)
        line = f"foobar,,,{number},10"
        l += 2 + len(line)
        body.append(line)
        if l > byte_size:
            break

    data = "\r\n".join(body).encode("utf-8")
    return header + data, row_count


class TestStringMethods(unittest.TestCase):
    def test_post_receive_lz4_block(self):
        data, row_count = generate_csv(10000)
        compressed_data = lz4.block.compress(data)
        post_headers = {'Content-Encoding': 'lz4', 'X-QCache-row-count-hint': str(row_count)}

        client.post("test-key", compressed_data, content_type='text/csv', post_headers=post_headers)
        resp = client.get("test-key", {}, query_headers={"Accept-Encoding": "lz4", "Accept": "application/json"})

        self.assertEqual(resp.encoding, "lz4")
        resp_data = lz4.block.decompress(resp.content)
        self.assertEqual(row_count, len(json.loads(resp_data.decode("utf-8"))))


    def test_post_receive_lz4_frame(self):
        data, row_count = generate_csv(10000)
        compressed_data = lz4.frame.compress(data)
        post_headers = {'Content-Encoding': 'lz4-frame'}

        client.post("test-key2", compressed_data, content_type='text/csv', post_headers=post_headers)
        resp = client.get("test-key2", {}, query_headers={"Accept-Encoding": "lz4-frame", "Accept": "application/json"})

        self.assertEqual(resp.encoding, "lz4-frame")
        resp_data = lz4.frame.decompress(resp.content)
        self.assertEqual(row_count, len(json.loads(resp_data.decode("utf-8"))))


if __name__ == '__main__':
    unittest.main()
