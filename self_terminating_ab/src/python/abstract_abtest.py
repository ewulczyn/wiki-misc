from numpy.random import binomial, multinomial, beta, dirichlet
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint
import pandas as pd
from abtest_util import SimStream, DonationProb, EmpiricalDonationProb
from abc import ABCMeta, abstractmethod

class ABTest(object):
    __metaclass__ = ABCMeta
    def __init__(self, a_stream, b_stream, test_interval, max_run):
        self.a_stream = a_stream 
        self.b_stream = b_stream
        self.max_run = max_run
        self.test_interval = test_interval # perform a test every interval records
        self.a_estimator = None  
        self.b_estimator = None
        self.has_run = False


    @abstractmethod
    def evaluate_stopping_criterium(self):
        pass

    def run(self):
        if self.has_run:
            print "This test already ran"
            return

        while True:
            a_records = self.a_stream.get_next_records(self.test_interval)
            b_records =self.b_stream.get_next_records(self.test_interval)
            self.a_estimator.update(a_records)
            self.b_estimator.update(b_records)
            result = self.evaluate_stopping_criterium()
            if result != 'continue':
                self.has_run = True
                return result




def expected_results(TestClass, params, iters):
    num_choose_A = 0.0
    unknown_count = 0.0
    run_times = []

    for i in range(iters):
        t = TestClass(*params)
        result = t.run()

        if result == 'A':
            num_choose_A+=1
        elif result == 'unknown':
            unknown_count+=1
        run_times.append(max(t.a_estimator.N,t.b_estimator.N))

    return num_choose_A/iters, unknown_count/iters, np.array(run_times)



def expected_results_by_params(TestClass, params_list, iters):
    p_choose_As = []
    p_unknowns = []
    run_times = []


    for params in params_list:
        p_choose_A, p_unknown, run_time = expected_results(TestClass, params, iters)
        run_times.append(run_time)
        p_choose_As.append(p_choose_A)
        p_unknowns.append(p_unknown)

    return p_choose_As, p_unknowns, run_times



def expected_results_by_lift(TestClass, params, iters, p_hat, lifts, fig_name = None):

    
    # see how you would do in practice
    run_times_list = [] #{"lower": [], "upper":[], "mean": []}
    p_A_betters =  {"lower": [], "upper":[], "mean": []}
    p_unknowns = {"lower": [], "upper":[], "mean": []}

    (lower, mean, upper) = p_hat.p_donate_ci(10)

    base_test = TestClass(*params)

    for lift in lifts:
        print lift

        #lower
        p_B = p_hat.change_p_donate(lower)
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream


        p_better, p_unknown, time = expected_results(TestClass, params, iters)
        run_times_list.append(time)
        p_A_betters['lower'].append(p_better)
        p_unknowns['lower'].append(p_unknown)


        # mean
        p_B = p_hat
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream

        #p_better, p_unknown, time = expected_results(TestClass, params, iters)
        #run_times_list.append(time)
        p_A_betters['mean'].append(p_better)
        p_unknowns['mean'].append(p_unknown)

        


        #upper
        p_B = p_hat.change_p_donate(upper)
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream

        #p_better, p_unknown, time = expected_results(TestClass, params, iters)
        #times['upper'].append(time)
        p_A_betters['upper'].append(p_better)
        p_unknowns['upper'].append(p_unknown)


    avg_run_times = np.array([np.mean(run_times) for run_times in run_times_list])
    lower = [np.percentile(run_times, 5) for run_times in run_times_list]
    upper = [np.percentile(run_times, 95) for run_times in run_times_list]


    fig = plt.figure(figsize = (13, 8))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)

    data = zip(lifts, p_A_betters['mean'], p_unknowns['mean'], avg_run_times)
    df = pd.DataFrame.from_records(data, columns=['% lift A over B', 'P(Choosing A)', 'P(Unknown)', 'Avg Time'])
    

    ax1.set_ylim([-0.1,1.1])
    ax1.plot(lifts, p_A_betters['mean'], label= 'P(A better) ')
    ax1.plot(lifts, p_A_betters['lower'], label= 'lower', alpha=0.31)
    ax1.plot(lifts, p_A_betters['upper'], label= 'upper', alpha=0.31)


    ax1.plot(lifts, p_unknowns['mean'], label='P(unknown)')
    ax1.set_xlabel('lift')
    ax1.set_ylabel(' prob')
    ax1.legend(loc = 4)

    ax2.plot(lifts, avg_run_times, label= 'avg time')
    ax2.fill_between(lifts, lower, upper, alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)
    ax2.set_xlabel('lift')
    ax2.set_ylabel('num impressions per banner')
    ax2.legend()
    plt.show()
    if fig_name:
        fig.savefig(fig_name)

    return df