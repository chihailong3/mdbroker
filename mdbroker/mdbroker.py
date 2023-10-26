

"""
Majordomo Protocol broker
A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8

Author: Min RK <benjaminrk@gmail.com>
Based on Java example by Arkadiusz Orzechowski
"""

import logging
import sys
import time
from binascii import hexlify

import zmq

# local
import MDP
#from zhelpers import dump
def dump(any):
    #print(any)
    pass
class Service(object):
    """a single Service"""
    name = None # Service name
    requests = None # List of client requests
    waiting = None # List of waiting workers

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []
        pass
    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

class Worker(object):
    """a Worker, idle or active"""
    identity = None # hex Identity of worker
    address = None # Address to route to
    service = None # Owning service, if known
    expiry = None # expires at this point, unless heartbeat

    def __init__(self, identity, address, lifetime):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + 1e-3*lifetime

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


class MajorDomoBroker(object):
    """
    Majordomo Protocol broker
    A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8
    """

    # We'd normally pull these from config data
    INTERNAL_SERVICE_PREFIX = b"mmi."
    HEARTBEAT_LIVENESS = 3 # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500 # msecs
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    # ---------------------------------------------------------------------

    ctx = None # Our context
    socket = None # Socket for clients & workers
    ipcsocket = None  # Socket for ipc in same machine
    poller = None # our Poller

    heartbeat_at = None# When to send HEARTBEAT
    services = None # known services
    workers = None # known workers
    waiting = None # idle workers

    verbose = False # Print activity to stdout

    # ---------------------------------------------------------------------


    def __init__(self, verbose=False):
        """Initialize broker state."""
        self.verbose = verbose
        self.services = {}
        self.workers = {}
        self.waiting = [] #All alived worker in herer !!!!!
        self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.ipcsocket = self.ctx.socket(zmq.SUB)
        self.socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        logging.basicConfig(format="%(asctime)s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S",
                            level=logging.INFO)

    # ---------------------------------------------------------------------

    def mediate(self):
        """Main broker work happens here"""
        while True:
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                break # Interrupted
            
            if items:
                msg = self.socket.recv_multipart()
                if self.verbose:
                    logging.info("I: received message:")
                    dump(msg)

                sender = msg.pop(0)
                empty = msg.pop(0)
                assert empty == b''
                header = msg.pop(0)

                if (MDP.C_CLIENT == header):
                    self.process_client(sender, msg)
                elif (MDP.W_WORKER == header):
                    self.process_worker(sender, msg)
                else:
                    logging.error("E: invalid message:")
                    dump(msg)

            self.purge_workers()
            self.send_heartbeats()
    def get_activate_worker_service(self):
        res = []
        for worker  in self.workers.values():
            now = time.time()
            if worker.expiry > now:  # worker is not outdate
                res.append(worker.service.name)
        return  res
        pass
    def destroy(self):
        """Disconnect all workers, destroy context."""
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.ctx.destroy(0)


    def process_client(self, sender, msg):
        """Process a request coming from a client."""
        assert len(msg) >= 2 # Service name + body
        service_name = msg.pop(0)
        # Set reply return address to client sender
        msg = [sender, b''] + msg
        if service_name.startswith(self.INTERNAL_SERVICE_PREFIX):
            self.service_internal(service_name, msg)
        else:
            self.dispatch(self.require_service_client(service_name), msg,service_name)


    def process_worker(self, sender, msg):
        """Process message sent to us by a worker."""
        assert len(msg) >= 1 # At least, command

        command = msg.pop(0)

        worker_ready = hexlify(sender) in self.workers

        worker = self.require_worker(sender)# 通过sender id 找到worker实例

        if (MDP.W_READY == command):
            assert len(msg) >= 1 # At least, a service name
            service = msg.pop(0)
            # Not first command in session or Reserved service name
            if (worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX)):
                self.delete_worker(worker, True)
            else:
                # Attach worker to service and mark as idle
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif (MDP.W_REPLY == command):
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0) # ?
                msg = [client, b'', MDP.C_CLIENT, worker.service.name] + msg
                self.socket.send_multipart(msg)
                self.worker_waiting(worker)
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_HEARTBEAT == command):
            if (worker_ready):
                worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_DISCONNECT == command):
            self.delete_worker(worker, False)
        else:
            logging.error("E: invalid message:")
            dump(msg)

    def delete_worker(self, worker, disconnect):
        """Deletes worker from all data structures, and deletes worker."""
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, MDP.W_DISCONNECT, None, None)

        if worker.service is not None:
            worker.service.waiting.remove(worker)


        self.workers.pop(worker.identity)
        #update services names

    def require_worker(self, address):
        """Finds the worker (creates if necessary)."""
        print("sender:",address)
        assert (address is not None)
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if (worker is None):
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY)
            print(worker)
            self.workers[identity] = worker
            if self.verbose:
                logging.info("I: registering new worker: %s", identity)
                logging.info("I: After registering new worker {}  ".format(self.workers ))

        return worker

    def require_service(self, name):
        """Locates the service (creates if necessary)."""
        assert (name is not None)
        service = self.services.get(name)
        if (service is None):
            service = Service(name)
            self.services[name] = service
            logging.info("I: After require_service new services:{} !".format(self.services.keys()))

        return service

    def require_service_client(self, name):
        """A queue composed of many workers under each service class"""
        """Get service class by service name !"""
        """Locates the service (creates if necessary)."""
        assert (name is not None)
        service = self.services.get(name)
        if service ==None:
            return None
        services = self.services.keys()
        if len(services) and service.name  in services:
            return service
        else :
            return None



    def bind(self, endpoint):
        """Bind broker to endpoint, can call this multiple times.

        We use a single socket for both clients and workers.
        """
        self.socket.bind(endpoint)
        logging.info("I: MDP broker/0.1.1 is active at %s", endpoint)
        ipc_endpoint = "ipc:///VCserver/ddd/0"
        #self.ipcsocket.bind(ipc_endpoint)
        #logging.info("I: MDP broker/0.1.1 is active at %s", ipc_endpoint)

    def service_internal(self, service, msg):
        """Handle internal service according to 8/MMI specification"""
        returncode = b"501"
        if b"mmi.service" == service:
            name = msg[-1]
            returncode = b"200" if name in self.services else b"404"
        msg[-1] = returncode

        # insert the protocol header and service name after the routing envelope ([client, ''])
        msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
        self.socket.send_multipart(msg)

    def send_heartbeats(self):
        """Send heartbeats to idle workers if it's time"""
        if (time.time() > self.heartbeat_at):
            for worker in self.waiting:
                self.send_to_worker(worker, MDP.W_HEARTBEAT, None, None)

            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL

    def purge_workers(self):
        """Look for & kill expired workers.

        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        print('enter purge_workers services:{}'.format(self.services.keys()))
        #while len(self.waiting)>0:
        for i ,w  in enumerate(self.waiting):
            #w = self.waiting[0]
            now = time.time()
            #print("name {} expir{} now:{}".format(w.identity,w.expiry,now))
            if w.expiry < now:
                logging.info("I: deleting expired worker: %s", w.identity)
                self.delete_worker(w,False)
                self.waiting.pop(i)
                if w.identity in self.workers.keys():
                    self.workers.pop(w.identity)
            # else:
            #     break

        self.services = {}  # reset services
        for i,w in enumerate(self.waiting) :
            self.services[w.service.name] = w.service

        print('after purge_workersupdate uppdate services:{}'.format(self.services.keys()))

    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        # Queue to broker and service waiting lists
        logging.info("I: append   worker: %s", worker.identity)
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service, msg,service_name = None):
        """Dispatch requests to waiting workers as possible"""
        ###############################add new ################
        if service is None and service_name== b'askService':
            client = msg.pop(0)
            empty = msg.pop(0)  # ?

            bservices = bytes('{}'.format(self.services),'utf-8')
            msg = [client, b'', MDP.C_CLIENT, service_name] + [bservices]
            self.socket.send_multipart(msg)
            return
            pass
        if service is None   :  # If there have not this worker ,sending a msg to clent and return ;
            client = msg.pop(0)
            empty = msg.pop(0)  # ?
            msg = [client, b'', MDP.C_CLIENT, service_name] + [b"Not this worker"]
            self.socket.send_multipart(msg)
            return
            pass
        #else service is not Noe
        # assert (service is not None)

        ############################
        if msg is not None:# Queue message if any
            service.requests.append(msg)
        self.purge_workers()
        while service.waiting and service.requests: #worker  and  work
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            self.send_to_worker(worker, MDP.W_REQUEST, None, msg)

    def send_to_worker(self, worker, command, option, msg=None):
        """Send message to worker.
        If message is provided, sends that message.
        """

        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        # Stack routing and protocol envelopes to start of message
        # and routing envelope
        if option is not None:
            msg = [option] + msg
        msg = [worker.address, b'', MDP.W_WORKER, command] + msg

        if self.verbose:
            logging.info("I: sending %r to worker", command)
            dump(msg)

        self.socket.send_multipart(msg)


def main():
    """create and start new broker"""
    verbose = '-v' in sys.argv
    broker = MajorDomoBroker(verbose)
    broker.bind("tcp://*:5555")
    broker.mediate()

if __name__ == '__main__':
    main()
