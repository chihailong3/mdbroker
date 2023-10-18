"""
Majordomo Protocol client example. Uses the mdcli API to hide all MDP aspects

Author : Min RK <benjaminrk@gmail.com>

"""

import random
import sys
import time

from mdcliapi import MajorDomoClient
services = [b"echo1122",b"echo11"]
def main():
    verbose = '-v' in sys.argv
    client = MajorDomoClient("tcp://192.168.117.106:5555", verbose)
    count = 0
    while count < 10000:
        request = b"Hello world"
        i = random.randint(0,1)
        try:
            reply = client.send(services[i], request)
            time.sleep(0.1)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    print ("%i requests/replies processed" % count)

if __name__ == '__main__':
    main()
