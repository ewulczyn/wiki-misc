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

def dt_to_day(dt):
    return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)


class IntervalCruncher():
    def __init__(self, base_dir, hour_lag, redo = False):
        self.base_dir = base_dir
        self.hive_query_file = os.path.join(base_dir,'interval_cruncher_queries', 'hive_query.sql')
        self.hive_create_file = os.path.join(base_dir,'interval_cruncher_queries',  'hive_create.sql')
        self.hive_drop_file = os.path.join(base_dir, 'interval_cruncher_queries', 'hive_drop.sql')
        self.hour_lag = hour_lag
        self.state_dir = os.path.join(base_dir, 'interval_cruncher_state')

        if redo:
            print "DELETING ALL DATA"
            with open (self.hive_drop_file, "r") as f:
                hive_query = f.read()
            cmd = """hive  -e \" """ +hive_query+ """ \" """ 
            os.system(cmd)
            os.system('rm %s/20*' % self.state_dir)

        print "CREATING TABLE IF NOT THERE"
        with open (self.hive_create_file, "r") as f:
            hive_query = f.read()
        cmd = """hive  -e \" """ +hive_query+ """ \" """ 
        if os.system(cmd) != 0:
            print hive_query
            print "BUG in create query"
            sys.exit()


    def hour_been_crunched(self, dt):
        return os.path.isfile(os.path.join(self.state_dir, dt_to_hour(dt)))



    def crunch_hour(self, dt):

        if self.hour_been_crunched(dt):
            print "YOU ALREADY CRUNCHED THIS INTERVAL"
            return

        else:
            os.system('touch %s' % os.path.join(os.path.join(self.state_dir, dt_to_hour(dt))))
            hive_query_params = {'year': dt.year, 'month': dt.month, 'day':dt.day, 'hour':dt.hour }

            with open (self.hive_query_file, "r") as f:
                hive_query_prototype=f.read()
            print hive_query_prototype
            hive_query = hive_query_prototype % hive_query_params
            print hive_query
            cmd = """hive  -e \" """ +hive_query+ """ \" """ 
            if os.system(cmd) != 0:
                print "CRUNCHING FAILED!!!!1"
                os.system('rm %s' % os.path.join(os.path.join(self.state_dir, dt_to_hour(dt))))



    def crunch_past_hours(self, num_hours):
        base = datetime.utcnow()
        date_list = [base - timedelta(hours= x + self.hour_lag) for x in range(0, num_hours)]
        for dt in date_list:
            self.crunch_hour(dt)

    

    def crunch_curr_hour(self, lock = True):

        dt = datetime.utcnow() - timedelta(hours = self.hour_lag)

        if lock:
            if os.path.isfile(os.path.join(self.state_dir, '.lock')):
                exit() 
            os.system('touch %s' % os.path.join(self.state_dir, '.lock'))
            self.crunch_hour(dt)
            os.system('rm %s' % os.path.join(self.state_dir, '.lock'))
        else:
            self.crunch_hour(dt)


#############



    def day_been_crunched(self, dt):
            return os.path.isfile(os.path.join(self.state_dir, dt_to_day(dt)))



    def crunch_day(self, dt):

        if self.day_been_crunched(dt):
            print "YOU ALREADY CRUNCHED THIS HOUR"
            return

        else:
            os.system('touch %s' % os.path.join(os.path.join(self.state_dir, dt_to_day(dt))))
            hive_query_params = {'year': dt.year, 'month': dt.month, 'day':dt.day}

            with open (self.hive_query_file, "r") as f:
                hive_query_prototype=f.read()
            print hive_query_prototype
            hive_query = hive_query_prototype % hive_query_params
            print hive_query
            cmd = """hive  -e \" """ +hive_query+ """ \" """ 
            if os.system(cmd) != 0:
                print "CRUNCHING FAILED!!!!1"
                os.system('rm %s' % os.path.join(os.path.join(self.state_dir, dt_to_day(dt))))



    def crunch_curr_day(self, lock = True):

        dt = datetime.utcnow() - timedelta(hours = self.hour_lag)

        if lock:
            if os.path.isfile(os.path.join(self.state_dir, '.lock')):
                exit() 
            os.system('touch %s' % os.path.join(self.state_dir, '.lock'))
            self.crunch_day(dt)
            os.system('rm %s' % os.path.join(self.state_dir, '.lock'))
        else:
            self.crunch_day(dt)


    def crunch_past_days(self, num_days): #cron at run at 3 utc
            base = datetime.utcnow()
            date_list = [base - timedelta(hours= 24*x + self.hour_lag) for x in range(0, num_days)]
            for dt in date_list:
                self.crunch_day(dt)



def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--hour_lag', type=int, dest='hour_lag', help='an integer for the accumulator')
    parser.add_argument('--base_dir',dest='base_dir', help='path to the interval_cruncher dirs)')
    parser.add_argument('--unit', dest='unit', help='time unit')
    parser.add_argument('--num_past_units', type=int,  dest='num_past_units', help='num units', default=0)


    args = parser.parse_args()
    h = IntervalCruncher(args.base_dir,  args.hour_lag)

    if args.num_past_units == 0:

        if args.unit == 'day':
            h.crunch_curr_day()
        elif args.unit == 'hour':
            h.crunch_curr_hour()
    else:
        print args.unit
        if args.unit == 'day':
            h.crunch_past_days(args.num_past_units)
        elif args.unit == 'hour':
            h.crunch_past_hours(args.num_past_units)


if __name__ == '__main__':
    main()