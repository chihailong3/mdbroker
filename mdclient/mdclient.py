"""
Majordomo Protocol client example. Uses the mdcli API to hide all MDP aspects

Author : Min RK <benjaminrk@gmail.com>

"""

import random
import sys
import time

from mdcliapi import MajorDomoClient
from mdcliapi2 import MajorDomoClient as MajorDomoClient1
services = [b"echo1122",b"echo11",b"mmi.service"]

# python3
import ast
import json
byte_str = b"{b'one': 1, 'two': 2}"
dict_str = byte_str.decode("UTF-8")
mydata = ast.literal_eval(dict_str)
print(repr(mydata))
TRIP_COUNTS = 10000
def main():

    verbose = '-v' in sys.argv
    verbose = False
    client = MajorDomoClient("tcp://192.168.117.106:5555", verbose)
    count = 0
    start = time.time()
    print("Synchronous round-trip test...")
    while count < TRIP_COUNTS:
        request = b"Hello world"
        request = b"Hello"
        i =1 # random.randint(0,2)
        try:
            reply = client.send(services[i], request)
            #mydata = ast.literal_eval(reply[0])
            # mydata = json.loads(reply[0].decode("utf-8").replace("'", '"'))
            # print('')

            #print("Reply: ",reply)
            #time.sleep(0.1)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    #print ("%i requests/replies processed" % count)
    print(" %d calls/second" % (TRIP_COUNTS / (time.time() - start)))

    print("Asynchronous round-trip test...")
    client = MajorDomoClient1("tcp://192.168.117.106:5555", False)
    #############Asyn call################
    count = 0
    start = time.time()
    for r in range(TRIP_COUNTS) :
        request = b"Hello world"
        request = b"Hello"
        i = 1  # random.randint(0,2)
        try:
            client.send(services[i], request)
            reply = client.recv()
            print("Reply: ", reply)
            #time.sleep(0.1)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    # for r in range(TRIP_COUNTS):
    #     reply = client.recv()
    #     print("Reply: ", reply)

    #print("%i requests/replies processed" % count)
    print(" %d calls/second" % (TRIP_COUNTS / (time.time() - start)))
    pass

if __name__ == '__main__':
    main()
