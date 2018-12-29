from qclient import QClient
import lz4.frame
import lz4.block
import time

client = QClient(node_list=('http://localhost:8888',))


def generate_csv(byte_size):
    header = b'abc,def,ghi,jkl,mno\r\n'

    body = []
    line = "foobar,,,10.12345,10"
    l = len(header)
    while True:
        l += 2 + len(line)
        body.append(line)
        if l > byte_size:
            break

    data = "\r\n".join(body).encode("utf-8")
    return header + data


def post_data(data_type, headers, data, orig_size):
    t0 = time.time()
    client.post("test-key", data, content_type='text/csv', post_headers=headers)
    print("Post duration {} {}: {}".format(data_type, orig_size, time.time() - t0))


def get_data(data_type, headers, orig_size):
    t0 = time.time()
    resp = client.get("test-key", {}, query_headers=headers)
    print("Get duration {} {}: {}".format(data_type, orig_size, time.time() - t0))
    return resp


def compressed_benchmark(size):
    import lz4.frame
    data = generate_csv(size)
    t0 = time.time()
    compressed_data = lz4.frame.compress(data, block_linked=False)
    print("Compress duration {}: {}".format(size, time.time() - t0))

    post_headers = {'Content-Encoding': 'lz4'}
    post_data("compressed", post_headers, compressed_data, size)
    resp = get_data("compressed", {"Accept-Encoding": "lz4"}, size)

    t0 = time.time()
    data = lz4.frame.decompress(resp.content)
    print("Decompress duration {}: {}".format(len(resp.content), time.time() - t0))

    t0 = time.time()

    # store_size=True does a lot to speed up decompression, ~20x
    compressed_data = lz4.frame.compress(data, block_linked=False, store_size=False)
    print("Python compress duration {}: {}".format(len(data), time.time() - t0))

    t0 = time.time()
    lz4.frame.decompress(compressed_data)
    print("Python decompress duration {}: {}".format(len(compressed_data), time.time() - t0))


def uncompressed_benchmark(size):
    data = generate_csv(size)
    post_data("uncompressed", {}, data, size)
    get_data("uncompressed", {}, size)


def compress_decompress_benchmark():
    data = generate_csv(10000000)
    t0 = time.time()
    compressed_data = lz4.block.compress(data)
    print("Block compress duration: {}".format(time.time() - t0))

    t0 = time.time()
    lz4.block.decompress(compressed_data)
    print("Block decompress duration: {}".format(time.time() - t0))

    t0 = time.time()
    compressed_data = lz4.frame.compress(data, block_linked=False)
    print("Frame compress duration: {}".format(time.time() - t0))

    t0 = time.time()
    data = lz4.frame.decompress(compressed_data)
    print("Frame decompress duration {}: {}".format(len(data), time.time() - t0))


sizes = (1000, 100000, 10000000)
for s in sizes:
    compressed_benchmark(s)
print("---------------------")
for s in sizes:
    uncompressed_benchmark(s)
print("---------------------")
compress_decompress_benchmark()

# Compress duration 1000: 1.5497207641601562e-05
# Post duration compressed 1000: 0.009674787521362305
# Get duration compressed 1000: 0.010808467864990234
# Decompress duration 103: 0.0002238750457763672
# Python compress duration 2836: 1.9073486328125e-05
# Python decompress duration 96: 0.00016999244689941406
# Compress duration 100000: 9.369850158691406e-05
# Post duration compressed 100000: 0.012645959854125977
# Get duration compressed 100000: 0.021214962005615234
# Decompress duration 1218: 0.014893770217895508
# Python compress duration 286336: 0.0002486705780029297
# Python decompress duration 1498: 0.010906696319580078
# Compress duration 10000000: 0.006943225860595703
# Post duration compressed 10000000: 0.6397826671600342
# Get duration compressed 10000000: 0.6576554775238037
# Decompress duration 112845: 0.9228959083557129
# Python compress duration 28636336: 0.0201413631439209
# Python decompress duration 144891: 0.8815534114837646
# ---------------------
# Post duration uncompressed 1000: 0.005550384521484375
# Get duration uncompressed 1000: 0.005549907684326172
# Post duration uncompressed 100000: 0.012236356735229492
# Get duration uncompressed 100000: 0.01726365089416504
# Post duration uncompressed 10000000: 0.6105444431304932
# Get duration uncompressed 10000000: 0.993727445602417
# ---------------------
# Block compress duration: 0.007385969161987305
# Block decompress duration: 0.007120609283447266
# Frame compress duration: 0.008198022842407227
# Frame decompress duration 10000009: 0.006821632385253906
