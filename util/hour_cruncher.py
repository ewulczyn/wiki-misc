import sys
import MySQLdb
import numpy as np
import collections
import re
import sys, traceback
import datetime
import os
from datetime import datetime, timedelta


def dt_to_hour(dt):
    return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+'T'+str(dt.hour)


class HourCruncher():
    def __init__(self, script_dir, redo = False):
        self.script_dir = script_dir
        print script_dir
        self.hive_query_file = os.path.join(script_dir, 'hive_query.sql')
        self.hive_create_file = os.path.join(script_dir, 'hive_create.sql')
        self.hive_drop_file = os.path.join(script_dir, 'hive_drop.sql')

        if redo:
            print "DELETING ALL DATA"
            with open (self.hive_drop_file, "r") as f:
                hive_query = f.read()
            cmd = """hive  -e \" """ +hive_query+ """ \" """ 
            os.system(cmd)
            os.system('rm %s/20*' % self.script_dir)

        print "CREATING TABLE IF NOT THERE"
        with open (self.hive_create_file, "r") as f:
            hive_query = f.read()
        cmd = """hive  -e \" """ +hive_query+ """ \" """ 
        if os.system(cmd) != 0:
            print hive_query
            print "BUG in create query"
            sys.exit()



    def been_crunched(self, dt):
        return os.path.isfile(os.path.join(self.script_dir, dt_to_hour(dt)))



    def crunch(self, dt):

        if self.been_crunched(dt):
            print "YOU ALREADY CRUNCHED THIS HOUR"
            return

        else:
            os.system('touch %s' % os.path.join(os.path.join(self.script_dir, dt_to_hour(dt))))
            hive_query_params = {'year': dt.year, 'month': dt.month, 'day':dt.day, 'hour':dt.hour }

            with open (self.hive_query_file, "r") as f:
                hive_query_prototype=f.read()

            hive_query = hive_query_prototype % hive_query_params
            print hive_query
            cmd = """hive  -e \" """ +hive_query+ """ \" """ 
            if os.system(cmd) != 0:
                print "CRUNCHING FAILED!!!!1"
                os.system('rm %s' % os.path.join(os.path.join(self.script_dir, dt_to_hour(dt))))



    def crunch_past(self, num_hours):
        base = datetime.utcnow()
        date_list = [base - timedelta(hours= x + 4) for x in range(0, num_hours)]
        for dt in date_list:
            self.crunch(dt)


    def crunch_curr(self, lock = True):

        dt = datetime.utcnow() - timedelta(hours = 3)

        if lock:
            if os.path.isfile(os.path.join(self.script_dir, '.lock')):
                exit() 
            os.system('touch %s' % os.path.join(self.script_dir, '.lock'))
            self.crunch(dt)
            os.system('rm %s' % os.path.join(self.script_dir, '.lock'))
        else:
            self.crunch(dt)


    
