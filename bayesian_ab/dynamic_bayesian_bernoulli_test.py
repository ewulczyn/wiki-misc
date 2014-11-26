from numpy.random import binomial, multinomial, beta, dirichlet
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pprint import pprint
from bayesian_bernouli import Banner, BernouliEstimator
from multiprocessing import Pool
import matplotlib.pyplot as plt




def estimate_baseline(p, conf, min_effect,  alpha = 1, beta = 1):
    interval = round((1/p) * 10)
    time = interval
    A = Banner(p, BernouliEstimator(alpha = 1, beta =1))
    A.run(interval)
    dist = A.estimator.get_distribution()
    m = np.mean(dist)
    l = np.percentile(dist, 100*min_effect)

    while(m-l > min_effect*l):
        time += interval
        A.run(interval)
        dist = A.estimator.get_distribution()
        m = np.mean(dist)
        l = np.percentile(dist, 100*min_effect)

    return time, m, l





def hit_conf_star(t):
    return hit_conf(*t)



def hit_conf(conf,p, lift, interval, consecutives = 1, max_run = float('inf')):

    A = Banner(p+(p*lift), BernouliEstimator(alpha = 1, beta = 1))
    B = Banner(p, BernouliEstimator(alpha = 1, beta = 1))
    time = 0
    hit_better = 0
    hit_worse = 0
    

    while(hit_better < consecutives and hit_worse < consecutives):
        time+=interval
        A.run(interval)
        B.run(interval)

        dist_a = A.estimator.get_distribution()
        dist_b = B.estimator.get_distribution()
        lift_dist = (dist_a - dist_b)/dist_b

        if np.mean(lift_dist > 0) > conf:
            hit_better += 1
        else:
            hit_better = 0

        if np.mean(lift_dist < 0) > conf:
            hit_worse += 1
        else:
            hit_worse = 0

        if max_run < time:
            break

    if hit_better < consecutives:
        return 0.0, time
    else:
        return 1.0, time




def prob_hit_conf(iters, conf,p, lift, interval, consecutives = 1,  max_run = float('inf')):
    times = []
    betters = []
    
    #for i in range(iters):
    #    better, time = hit_conf(conf,p, lift, interval, consecutives, max_run)
    #    betters.append(better)
    #    times.append(time)
    

    pool = Pool(5)
    res = pool.map(hit_conf_star, [(conf,p, lift, interval, consecutives, max_run) for i in range(iters)])
    betters = [t[0] for t in res]
    times = [t[1] for t in res]

    return np.mean(np.array(betters)), np.array(times)

    




if __name__ == '__main__':

    p = 0.0008
    conf = 0.95
    min_effect = 0.05
    time, p_hat, l = estimate_baseline(p, conf, min_effect)

    print time, p_hat, l

    interval = round(1/p_hat) * 100

    print "Interval", interval


    #estimate maximum run-time
    iters = 100

    better, times = prob_hit_conf(iters, conf,p_hat, min_effect, interval, consecutives = 1)

    print better
    print np.mean(times)
    print np.std(times)

    max_run = np.mean(times)+ 2*np.std(times)



    # see how you would do in practice
    lifts = [x/100.0 for x in range(-20, 20, 1)]
    times = []
    p_A_wins = []


    for lift in lifts:
        better, time = prob_hit_conf(iters, conf, p_hat, lift, interval,min_effect, consecutives = 1, max_run = max_run)
        times.append(time)
        p_A_wins.append(better)

    avg_time = [np.mean(time) for time in times]
    std_time = [np.std(time) for time in times]

    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)


    ax1.plot(lifts, p_A_wins, label= 'P(A better)')
    ax1.set_xlabel('lift')
    ax1.set_ylabel(' prob')
    ax1.legend()

    ax2.plot(lifts, avg_time, label= 'avg time')
    ax2.set_xlabel('lift')
    ax2.set_ylabel(' time')
    ax2.legend()
    plt.show()


    #prob_hit_conf_star((0.9, 0.00025, -0.40, 50000, 100, 1))

    #prob_hit_conf(0.9, 0.00025, 0.10, 50000, 200, 1)


    def ci(sample, level):
    a = (100.0-level)/2
    return (np.percentile(sample, a ), np.mean(sample), np.percentile(sample, 100-a))



