# -*- coding: UTF-8 -*-

import multiprocessing

import MySQLdb
import sys, traceback
import multiprocessing 

import inspect, os
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
os.sys.path.insert(0,parentdir) 
from record_impression_helpers import  get_params, insert_to_db

NUM_PROCS = 6 #I wish: multiprocessing.cpu_count()


class ImpressionRecorder(object):
    def __init__(self, numprocs):
        self.numprocs = numprocs
        self.inq = multiprocessing.Queue()
        self.outq = multiprocessing.Queue()

        fn = sys.stdin.fileno()

        self.pin = multiprocessing.Process(target=self.read_stdin, args=(fn,))
        self.pout = multiprocessing.Process(target=self.write_to_db, args=())
        self.ps = [ multiprocessing.Process(target=self.parse_message, args=())
                        for i in range(self.numprocs)]

        self.pin.start()
        self.pout.start()
        for p in self.ps:
            p.start()

        
    def read_stdin(self, fn):
        """Reads from stdin. The data is then sent over inqueue for the workers to do their
        thing.  At the end the input thread sends a 'STOP' message for each
        worker.

        """
        sys.stdin = os.fdopen(fn)

        while True:
            self.inq.put(sys.stdin.readline())


    def parse_message(self):
        """
        Workers. Consume messages from inq and produce parameters on outq
        """

        for line in iter(self.inq.get, "STOP"):

            if line == '':
                continue
            try:
                dt, user_agent, path, query_params = line.split('|')
                params = get_params(dt,query_params,user_agent=user_agent )
                self.outq.put(params)
            except:
                traceback.print_exc(file=sys.stdout)


    def write_to_db(self):
        """
        Flush params to db every couple records
        """
        iterator = iter(self.outq.get, "STOP")
        transform = lambda x: x
        insert_to_db('impressions', iterator, transform)
        


if __name__ == '__main__':
    ImpressionRecorder(NUM_PROCS)
