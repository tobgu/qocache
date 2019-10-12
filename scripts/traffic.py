from qclient import QClient
import time

inputS = "a,b,c,d,e,f,g,h\n"
inputS += "\n".join(5000 * ["1200,456,123.12345,a string,another string,9877654.2,1234567.12,77"])

print("Size of input = {}".format(len(inputS)))

c = QClient(node_list=["http://localhost:8888"], read_timeout=10.0)

t0 = time.time()
for x in range(100000000):
    c.post("key{}".format(x), inputS)
    if x % 100 == 0:
        avg_ms = round(1000*(time.time()-t0) / 100, 2)
        print("Total count: {}, mean req time: {} ms".format(x, avg_ms))
        t0 = time.time()
