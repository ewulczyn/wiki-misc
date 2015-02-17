import sys
import math

### First columns looks like [123, 213, 212312]
### First column returns [123:0,  213:1,  212312:2]. 

def calc_burst_score(pageviews):
  '''
  Dead simple trend algorithm used for demo
  Only needs the last 10 days of data
  '''
  num_days = float(len(pageviews))

  # ~Today's pageviews...
  y2 = pageviews[-1]
  # ~last weeks average pageviews
  y1 = sum(pageviews[-8:-1]) / 7.0

  # ~Significance factor based on previous week's pageviews
  weekly_pageviews = sum(pageviews[-8:-1])  
  # Simple baseline trend algorithm
  slope = y2/y1
  score = slope  * (1.0 + math.log(1.0 +int(weekly_pageviews)))
  return score  


pageviews = []
curr_group = [None, None, None, None]
print_result = False

for line in sys.stdin:
    row = line.rstrip('\n').split('\t')
    group = row[:-1]
    if group != curr_group:
      curr_group = group
      if print_result:
        score = calc_burst_score(pageviews)
        row[-1] = str(score)
        print '\t'.join(row)
      pageviews = []
      print_result = True
    else:
      pageviews.append(int(row[-1]))

