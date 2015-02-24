import sys
import math

### First columns looks like [123, 213, 212312]
### First column returns [123:0,  213:1,  212312:2]. 


def calc_burst_score(pageviews):
  '''
  Dead simple trend algorithm used for demo
  Only needs the last 10 days of data
  '''
  #cap at 8 days of data
  pageviews = pageviews[-8:]
  # the artilce may not have been visited every day of that week
  num_days = float(len(pageviews))
  if num_days ==1:
    slope = pageviews[0]
  else:
    # ~Today's pageviews...
    y2 = pageviews[-1]
    # ~last weeks average pageviews
    y1 = sum(pageviews[-8:-1]) / (num_days - 1.0)
    # Simple baseline trend algorithm
    slope = y2-y1
  # ~Significance factor based on number of pageviews
  weekly_pageviews = sum(pageviews)  
  score = slope  * (1.0 + math.log(1.0 +int(weekly_pageviews)))
  return score



def mean(Xs):
    return sum(Xs) / float(len(Xs))
    

def std(Xs, m):
    normalizer = float(len(Xs) - 1)
    return math.sqrt(sum((pow(x - m, 2) for x in Xs)) / normalizer)

  
def calc_intensity_and_slope(Y):

  if len(Y) ==1:
    return Y[0], 0.0
  X = range(len(Y))

  

  m_X = mean(X)
  m_Y = mean(Y)

  sum_xy = 0
  sum_sq_v_x = 0
  sum_sq_v_y = 0

  for (x, y) in zip(X, Y):
      var_x = x - m_X
      var_y = y - m_Y
      sum_xy += var_x * var_y
      sum_sq_v_x += pow(var_x, 2)
      sum_sq_v_y += pow(var_y, 2)
  try:
    r =  sum_xy / math.sqrt(sum_sq_v_x * sum_sq_v_y)
    b = r * (std(Y, m_Y) / std(X, m_X))
  except:
    b = 0.0
  return m_Y, b



def emit(pageviews):
  intensity, slope = calc_intensity_and_slope(pageviews)
  curr_group.append(str(intensity))
  curr_group.append(str(slope))
  print '\t'.join(curr_group)

pageviews = []
curr_group = None
print_result = False

for line in sys.stdin:
    row = line.rstrip('\n').split('\t')
    group = row[:-1]
    num_views = int(row[-1])
    if group != curr_group:
      if print_result:
        emit(pageviews)
      pageviews = []
      curr_group = group
      print_result = True
    pageviews.append(num_views)

if print_result:
  emit(pageviews)

