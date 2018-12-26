from qclient import QClient
import lz4.frame

client = QClient(node_list=('http://localhost:8888',))


def post_csv():
    data = """field1,field2
abc,def"""
    client.post("test-key", lz4.frame.compress(data.encode("utf-8")), post_headers={"Content-Encoding": "lz4"})


def get_csv():
    return client.get("test-key", {}, query_headers={"Accept-Encoding": "lz4"})


post_csv()
response = get_csv()
print(lz4.frame.decompress(response.content))

