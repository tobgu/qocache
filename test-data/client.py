import random

import requests
from qclient import QClient
import lz4.frame
import lz4.block
import time

# client = QClient(node_list=('https://localhost:8888',), verify='../tls/ca.pem', cert='../tls/host.pem')
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


def post_data(data_type, headers, data, orig_size):
    t0 = time.time()
    client.post("test-key", data, content_type='text/csv', post_headers=headers)
    print("Post duration {} {}: {}".format(data_type, orig_size, time.time() - t0))


def get_data(data_type, headers, orig_size):
    t0 = time.time()
    resp = client.get("test-key", {}, query_headers=headers)
    print("Get duration {} {}: {}".format(data_type, orig_size, time.time() - t0))
    return resp


def block_compressed_benchmark(data, row_count):
    t0 = time.time()
    compressed_data = lz4.block.compress(data)
    size = len(data)
    print("Block compress duration {}: {}".format(size, time.time() - t0))

    post_headers = {'Content-Encoding': 'lz4', 'X-QCache-row-count-hint': str(row_count)}
    post_data("block-compressed", post_headers, compressed_data, size)
    resp = get_data("block-compressed", {"Accept-Encoding": "lz4"}, size)

    t0 = time.time()
    lz4.block.compress(resp.content)
    print("Block decompress duration {}: {}".format(len(resp.content), time.time() - t0))

    t0 = time.time()
    compressed_data = lz4.block.compress(data)
    print("Python block compress duration {}: {}".format(len(data), time.time() - t0))

    t0 = time.time()
    lz4.block.decompress(compressed_data)
    print("Python block decompress duration {}: {}".format(len(compressed_data), time.time() - t0))


def block_compressed_post_load(data):
    compressed_data = lz4.block.compress(data)
    size = len(data)

    post_headers = {'Content-Encoding': 'lz4'}

    while True:
        post_data("block-compressed-load", post_headers, compressed_data, size)


def block_compressed_get_load(data):
    compressed_data = lz4.block.compress(data)
    size = len(data)

    post_headers = {'Content-Encoding': 'lz4'}
    post_data("block-compressed-load", post_headers, compressed_data, size)
    while True:
        get_data("block-compressed", {"Accept-Encoding": "lz4"}, size)


def frame_compressed_benchmark(data):
    t0 = time.time()
    compressed_data = lz4.frame.compress(data, block_linked=False)
    size = len(data)
    print("Frame compress duration {}: {}".format(size, time.time() - t0))

    post_headers = {'Content-Encoding': 'lz4-frame'}
    post_data("frame-compressed", post_headers, compressed_data, size)
    resp = get_data("frame-compressed", {"Accept-Encoding": "lz4-frame"}, size)

    t0 = time.time()
    data = lz4.frame.decompress(resp.content)
    print("Frame decompress duration {}: {}".format(len(resp.content), time.time() - t0))

    t0 = time.time()

    compressed_data = lz4.frame.compress(data)
    print("Python frame compress duration {}: {}".format(size, time.time() - t0))

    t0 = time.time()
    lz4.frame.decompress(compressed_data)
    print("Python frame decompress duration {}: {}".format(len(compressed_data), time.time() - t0))


def uncompressed_benchmark(data, row_count):
    post_data("uncompressed", {"X-QCache-row-count-hint": str(row_count)}, data, len(data))
    get_data("uncompressed", {}, len(data))


def compress_decompress_benchmark(data):
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
    lz4.frame.decompress(compressed_data)
    print("Frame decompress duration {}: {}".format(len(data), time.time() - t0))

    t0 = time.time()
    compressed_data = lz4.frame.compress(data, block_linked=False, store_size=False)
    print("Frame compress no size duration: {}".format(time.time() - t0))

    t0 = time.time()
    lz4.frame.decompress(compressed_data)
    print("Frame decompress no size duration {}: {}".format(len(data), time.time() - t0))


def requests_performance():
    # s = requests.Session()
    s = requests.Session()
    s.verify = '../tls/ca.pem'
    s.cert = '../tls/host.pem'

    # Setting this to false cuts requests overhead from 9-10 ms to 2 - 4 ms.
    s.trust_env = False
    for _ in range(100):
        t0 = time.time()
        resp = s.get("https://localhost:8888/qcache/status")
        print("Duration: {}".format(time.time() - t0))
        assert resp.status_code == 200


if False:
    requests_performance()

if True:
    sizes = (1000, 100000, 10000000)
    for s in sizes:
        print(f"\n----- {s} -----")
        csv_data, row_count = generate_csv(s)
        block_compressed_benchmark(csv_data, row_count)
    #    frame_compressed_benchmark(csv_data)
        uncompressed_benchmark(csv_data, row_count)
    #    compress_decompress_benchmark(csv_data)

if False:
    csv_data = generate_csv(1000)
    block_compressed_benchmark(csv_data)

if False:
    csv_data = generate_csv(10000)
    block_compressed_get_load(csv_data)

# ----- 1000 -----
# Block compress duration 1016: 3.4809112548828125e-05
# Post duration block-compressed 1016: 0.010013580322265625
# Get duration block-compressed 1016: 0.006348133087158203
# Block decompress duration 502: 1.430511474609375e-05
# Python block compress duration 1016: 1.7404556274414062e-05
# Python block decompress duration 423: 6.4373016357421875e-06
# Frame compress duration 1016: 2.574920654296875e-05
# Post duration frame-compressed 1016: 0.005291938781738281
# Get duration frame-compressed 1016: 0.00875997543334961
# Frame decompress duration 515: 0.0002288818359375
# Python frame compress duration 1016: 2.8371810913085938e-05
# Python frame decompress duration 514: 3.647804260253906e-05
# Post duration uncompressed 1016: 0.006818056106567383
# Get duration uncompressed 1016: 0.00618290901184082
# Block compress duration: 1.9550323486328125e-05
# Block decompress duration: 6.4373016357421875e-06
# Frame compress duration: 2.5033950805664062e-05
# Frame decompress duration 1016: 2.3365020751953125e-05
# Frame compress no size duration: 1.9788742065429688e-05
# Frame decompress no size duration 1016: 1.5735626220703125e-05
#
# ----- 100000 -----
# Block compress duration 99999: 0.0007016658782958984
# Post duration block-compressed 99999: 0.013032197952270508
# Get duration block-compressed 99999: 0.014141321182250977
# Block decompress duration 35022: 0.0008203983306884766
# Python block compress duration 99999: 0.0007052421569824219
# Python block decompress duration 28511: 0.00023317337036132812
# Frame compress duration 99999: 0.0005924701690673828
# Post duration frame-compressed 99999: 0.019430875778198242
# Get duration frame-compressed 99999: 0.024871110916137695
# Frame decompress duration 35032: 0.00883936882019043
# Python frame compress duration 99999: 0.0008690357208251953
# Python frame decompress duration 33211: 0.0006248950958251953
# Post duration uncompressed 99999: 0.01810288429260254
# Get duration uncompressed 99999: 0.018460750579833984
# Block compress duration: 0.0007646083831787109
# Block decompress duration: 0.00015592575073242188
# Frame compress duration: 0.0006260871887207031
# Frame decompress duration 99999: 0.0001850128173828125
# Frame compress no size duration: 0.00054931640625
# Frame decompress no size duration 99999: 0.0014286041259765625
#
# ----- 10000000 -----
# Block compress duration 10000015: 0.07544541358947754
# Post duration block-compressed 10000015: 0.6772036552429199
# Get duration block-compressed 10000015: 0.8411071300506592
# Block decompress duration 3473944: 0.09713077545166016
# Python block compress duration 10000015: 0.07901954650878906
# Python block decompress duration 2797066: 0.02633380889892578
# Frame compress duration 10000015: 0.06122398376464844
# Post duration frame-compressed 10000015: 0.701441764831543
# Get duration frame-compressed 10000015: 0.7850394248962402
# Frame decompress duration 3475857: 0.7355217933654785
# Python frame compress duration 10000015: 0.0748136043548584
# Python frame decompress duration 3275922: 0.06526613235473633
# Post duration uncompressed 10000015: 0.647383451461792
# Get duration uncompressed 10000015: 1.1222872734069824
# Block compress duration: 0.07514023780822754
# Block decompress duration: 0.016193151473999023
# Frame compress duration: 0.05902743339538574
# Frame decompress duration 10000015: 0.01868605613708496
# Frame compress no size duration: 0.05801844596862793
# Frame decompress no size duration 10000015: 0.14292049407958984


# QCache, for reference
# ----- 1000 -----
# Block compress duration 1000: 1.4066696166992188e-05
# Post duration block-compressed 1000: 0.018204689025878906
# Get duration block-compressed 1000: 0.024323463439941406
# Block decompress duration 491: 2.002716064453125e-05
# Python block compress duration 1000: 1.7881393432617188e-05
# Python block decompress duration 417: 7.867813110351562e-06
# Post duration uncompressed 1000: 0.01737213134765625
# Get duration uncompressed 1000: 0.04035592079162598
#
# ----- 100000 -----
# Block compress duration 100014: 0.0005407333374023438
# Post duration block-compressed 100014: 0.03321433067321777
# Get duration block-compressed 100014: 0.037641286849975586
# Block decompress duration 33103: 0.0006084442138671875
# Python block compress duration 100014: 0.0004901885986328125
# Python block decompress duration 28478: 0.0002918243408203125
# Post duration uncompressed 100014: 0.028759241104125977
# Get duration uncompressed 100014: 0.049143314361572266
#
# ----- 10000000 -----
# Block compress duration 10000018: 0.05651998519897461
# Post duration block-compressed 10000018: 1.061251163482666
# Get duration block-compressed 10000018: 2.4552574157714844
# Block decompress duration 3273599: 0.05962109565734863
# Python block compress duration 10000018: 0.05662846565246582
# Python block decompress duration 2796747: 0.027180910110473633
# Post duration uncompressed 10000018: 1.0426273345947266
# Get duration uncompressed 10000018: 3.5250532627105713
