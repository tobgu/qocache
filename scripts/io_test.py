from qclient import QClient
import lz4.frame
import lz4.block
import time
import argparse


def frame_lz4er(data):
    enc_data = data.encode("utf-8")
    t0 = time.time()
    result = lz4.frame.compress(enc_data, block_size=lz4.frame.BLOCKSIZE_MAX4MB, block_linked=False)
    print("LZ4 frame compression time: {}, bytes: {}".format(time.time()-t0, len(result)))
    return result


def block_lz4er(data):
    enc_data = data.encode("utf-8")
    t0 = time.time()
    result = lz4.block.compress(enc_data)
    print("LZ4 block compression time: {}, bytes: {}".format(time.time()-t0, len(result)))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload and query IO/serialization performance and compatibility test')
    parser.add_argument('--lz4-block-uploads', type=int, default=0)
    parser.add_argument('--lz4-block-queries', type=int, default=0)
    parser.add_argument('--lz4-frame-uploads', type=int, default=0)
    parser.add_argument('--lz4-frame-queries', type=int, default=0)
    parser.add_argument('--plain-uploads', type=int, default=0)
    parser.add_argument('--plain-queries', type=int, default=0)
    parser.add_argument('--line-count', type=int, default=1000)

    args = parser.parse_args()

    input_string = "a,b,c,d,e,f,g,h\n"
    input_string += "\n".join(args.line_count * ["1200,456,123.12345,a string,another string,9877654.2,1234567.12,77"])
    print("Size of input = {}".format(len(input_string)))

    c = QClient(node_list=["http://localhost:8888"], read_timeout=10.0)

    frame_uploads = max(args.lz4_frame_uploads, int(args.lz4_frame_queries > 0))
    for _ in range(frame_uploads):
        t0 = time.time()
        c.post("key_lz4_frame", frame_lz4er(input_string), post_headers={"Content-Encoding": "lz4-frame"})
        print("LZ4 frame upload time: {} s".format(time.time() - t0))

    for _ in range(args.lz4_frame_queries):
        t0 = time.time()
        r = c.get("key_lz4_frame", {}, query_headers={"Accept-Encoding": "lz4-frame"})
        qt = time.time() - t0
        t0 = time.time()
        dr = lz4.frame.decompress(r.content)
        print("LZ4 frame query time: {} s, decompress time: {}, comp size: {}, uncomp size: {}".format(qt, time.time() - t0, len(r.content), len(dr)))

    block_uploads = max(args.lz4_block_uploads, int(args.lz4_block_queries > 0))
    for _ in range(block_uploads):
        t0 = time.time()
        c.post("key_lz4_block", block_lz4er(input_string), post_headers={"Content-Encoding": "lz4"})
        print("LZ4 block upload time: {} s".format(time.time() - t0))

    for _ in range(args.lz4_block_queries):
        t0 = time.time()
        r = c.get("key_lz4_block", {}, query_headers={"Accept-Encoding": "lz4"})
        qt = time.time() - t0
        t0 = time.time()
        dr = lz4.block.decompress(r.content)
        print("LZ4 block query time: {} s, decompress time: {}, comp size: {}, uncomp size: {}".format(qt, time.time() - t0, len(r.content), len(dr)))

    plain_uploads = max(args.plain_uploads, int(args.plain_queries > 0))
    for _ in range(plain_uploads):
        t0 = time.time()
        c.post("key_plain", input_string)
        print("Plain upload time: {} s".format(time.time() - t0))

    for _ in range(args.plain_queries):
        t0 = time.time()
        r = c.get("key_plain", {})
        print("Plain query time: {} s, size: {}".format(time.time() - t0, len(r.content)))
