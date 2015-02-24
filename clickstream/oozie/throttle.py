import sys
import math

"""
This script is used for removing traffic from fast moving crawlers.
It addresses the scenario in which the crawler requests a page 
and then proceeds to requests pages linked from that page at higher
frequency than a human would, say, open tabs. It is myopic in the sense 
that it only operates on one minute of request at a time. 

Input:
Records of the schema: [ip, user_agent, referer, minute, second, uri_path], 
sorted by sorted by (ip, user_agent, referer, minute, second) 

When using this script as a hive transform function be sure to
distribute by (ip, user_agent, referer, minute) and sort by
(ip, user_agent, referer, minute, second) 
"""


def throttle(requests):
  """
  Computes the rate of requests with the same
  referer and compares it to a dynamic threshold.
  If the rate is too high, the set of requests is discarded.
  """
  second_index = 4
  num_requests = float(len(requests))

  # one pageview per minute is certainly acceptible
  if num_requests == 1.0:
    print '\t'.join(requests[0])
  # 20 requests per minute is unreasonable
  elif num_requests > 20.0:
    return
  # in between, compute the true rate
  else:
    try:
      start_second = float(requests[0][second_index]) 
    except:
      start_second = 0.0
    try:
      stop_second = float(requests[-1][second_index])
    except:
      stop_second = 59.0

    request_interval = 1.0 + stop_second - start_second 
    rate = num_requests / request_interval
    # compute rate that it would take a human to generate that many views
    max_rate = num_requests / (num_requests + 0.1*num_requests**2)
      
    if rate < max_rate:
      for p in requests:
        print '\t'.join(p)


def main():
  """
  Send each set of records with the same
  (ip, user_agent, referer, minute) fields to
  throttle function to determine if the
  requests should be dropped.
  """

  requests = []
  curr_group = None
  print_result = False

  for line in sys.stdin:
      row = line.rstrip('\n').split('\t')
      if len(row) != 6:
        continue
      group = row[:4]
      if group != curr_group:
        if print_result:
          throttle(requests)
        requests = []
        curr_group = group
        print_result = True
      requests.append(row)

  if print_result:
    throttle(requests)


if __name__ == '__main__':
  main()