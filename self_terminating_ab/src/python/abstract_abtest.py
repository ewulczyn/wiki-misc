import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from abtest_util import SimStream
from abc import ABCMeta, abstractmethod

class ABTest(object):
    """
    This is the base class for dynamically
    terminating AB tests. The idea is that you define
    a stopping crietion and evaluate it every n records
    until you get the stop signal.
    """
    __metaclass__ = ABCMeta
    def __init__(self, a_stream, b_stream, test_interval, max_run):
        self.a_stream = a_stream            # a banner data stream object for banner A
        self.b_stream = b_stream            # a banner data stream object for banner B
        self.max_run = max_run              # the maximum number of samples per banner
        self.test_interval = test_interval  # evalaute stopping criterion every test_interval records
        self.a_estimator = None             # an object that collects stats on banner A
        self.b_estimator = None             # an object that collects stats on banner B
        self.has_run = False                # flag to see if the test has already been run once


    def run(self):
        """
        This function runs the banners for test_interval records
        Until the evaluate_stopping_criterium function returns a winner
        or the maximum sample size is reached 
        """
        if self.has_run:
            print "This test already ran"
            return

        while True:
            a_records = self.a_stream.get_next_records(self.test_interval)
            b_records = self.b_stream.get_next_records(self.test_interval)
            self.a_estimator.update(a_records)
            self.b_estimator.update(b_records)
            result = self.evaluate_stopping_criterium()
            if result != 'continue':
                self.has_run = True
                return result

    @abstractmethod
    def evaluate_stopping_criterium(self):
        """
        Each child class needs to define a criterion for stopping the test
        """
        pass


def expected_results(TestClass, params, iters):
    """
    Evaluates a test with the same parameters multiple times
    to get the expected results.

    Args:
        TestClass: AB Test Class
        params: parmaters for instantiating AB Test class
        iters: number of times to run the Test object with the set of params

    Returns:
    Prob(A wins), P(unkown winner), list of run times

    """
    num_choose_A = 0.0
    unknown_count = 0.0
    run_times = []

    for i in range(iters):
        t = TestClass(*params)
        result = t.run()

        if result == 'A':
            num_choose_A += 1
        elif result == 'unknown':
            unknown_count += 1
        run_times.append(max(t.a_estimator.N, t.b_estimator.N))

    return num_choose_A/iters, unknown_count/iters, np.array(run_times)


def expected_results_by_lift(TestClass, params, iters, p_hat, lifts, fig_name=None):

    """
    This function generates plots that show the expected results
    of the AB test as you change the lift that banner A has over
    banner B.
    """

    # see how you would do in practice
    run_times_list = []
    p_A_betters = {"lower": [], "upper":[], "mean": []}
    p_unknowns = {"lower": [], "upper":[], "mean": []}

    (lower, mean, upper) = p_hat.p_donate_ci(10)

    for lift in lifts:
        print lift
        #lower
        p_B = p_hat.change_p_donate(lower)
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream
        p_better, p_unknown, time = expected_results(TestClass, params, iters)
        p_A_betters['lower'].append(p_better)
        p_unknowns['lower'].append(p_unknown)

        # mean
        p_B = p_hat
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream
        p_better, p_unknown, time = expected_results(TestClass, params, iters)
        run_times_list.append(time)
        p_A_betters['mean'].append(p_better)
        p_unknowns['mean'].append(p_unknown)

        #upper
        p_B = p_hat.change_p_donate(upper)
        params[0] = SimStream(p_B.lift(lift)) #a_stream
        params[1] = SimStream(p_B) #b_stream
        p_better, p_unknown, time = expected_results(TestClass, params, iters)
        p_A_betters['upper'].append(p_better)
        p_unknowns['upper'].append(p_unknown)

    lifts = np.array(lifts)*100

    avg_run_times = np.array([np.mean(run_times) for run_times in run_times_list])
    lower = [np.percentile(run_times, 5) for run_times in run_times_list]
    upper = [np.percentile(run_times, 95) for run_times in run_times_list]


    fig = plt.figure(figsize=(13, 8))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)

    data = zip(lifts, p_A_betters['mean'], p_unknowns['mean'], avg_run_times)
    columns = ['% lift A over B', 'P(Choosing A) Median', 'P(Unknown) Median', 'Avg Time']
    df = pd.DataFrame.from_records(data, columns=columns)

    ax1.set_ylim([-0.1, 1.1])
    ax1.plot(lifts, p_A_betters['mean'], label='P(A wins) median')
    ax1.plot(lifts, p_A_betters['lower'], label='P(A wins) lower', alpha=0.31)
    ax1.plot(lifts, p_A_betters['upper'], label='P(A wins) upper', alpha=0.31)

    ax1.plot(lifts, p_unknowns['mean'], label='P(unknown)')
    ax1.set_xlabel('percent lift')
    ax1.set_ylabel('prob')
    ax1.legend(loc=4)

    ax2.set_xlim([lifts[0], lifts[-1]])
    ax2.plot(lifts, avg_run_times, label='avg time')
    ax2.fill_between(lifts, lower, upper, alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75', linewidth=0)
    ax2.set_xlabel('percent lift')
    ax2.set_ylabel('impressions per banner')
    ax2.legend()
    plt.show()
    if fig_name:
        fig.savefig(fig_name)

    return df





def expected_results_by_interval(TestClass, params, iters, p_hat, lifts, n1, n2, n3, fig_name=None):

    """
    This function generates plots that show the expected results
    of the AB test as you change the lift that banner A has over
    banner B.
    """

    # see how you would do in practice
    run_times_list = {"lower": [], "upper":[], "mean": []}
    p_A_betters = {"lower": [], "upper":[], "mean": []}

    (lower, mean, upper) = p_hat.p_donate_ci(10)

    for lift in lifts:
        print lift

        p_B = p_hat

        # mean
        new_params = list(params)
        new_params[0] = SimStream(p_B.lift(lift)) #a_stream
        new_params[1] = SimStream(p_B) #b_stream
        new_params[2] = n1
        p_better, p_unknown, time = expected_results(TestClass, new_params, iters)
        run_times_list['mean'].append(time)
        p_A_betters['mean'].append(p_better)


        #lower
        new_params = list(params)
        new_params[0] = SimStream(p_B.lift(lift)) #a_stream
        new_params[1] = SimStream(p_B) #b_stream
        new_params[2] = n2
        p_better, p_unknown, time = expected_results(TestClass, new_params, iters)
        p_A_betters['lower'].append(p_better)
        run_times_list['lower'].append(time)
        

        #upper
        new_params = list(params)
        new_params[0] = SimStream(p_B.lift(lift)) #a_stream
        new_params[1] = SimStream(p_B) #b_stream
        new_params[2] = n3
        p_better, p_unknown, time = expected_results(TestClass, new_params, iters)
        p_A_betters['upper'].append(p_better)
        run_times_list['upper'].append(time)

    lifts = np.array(lifts)*100

    avg_run_times_mean = np.array([np.mean(run_times) for run_times in run_times_list['mean']])
    avg_run_times_upper = np.array([np.mean(run_times) for run_times in run_times_list['upper']])
    avg_run_times_lower = np.array([np.mean(run_times) for run_times in run_times_list['lower']])


    fig = plt.figure(figsize=(13, 8))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)



    ax1.set_ylim([-0.1, 1.1])
    ax1.plot(lifts, p_A_betters['lower'], label='P(A wins) n = %d' % n1)
    ax1.plot(lifts, p_A_betters['mean'], label='P(A wins) n = %d' % n2)
    ax1.plot(lifts, p_A_betters['upper'], label='P(A wins) n = %d' % n3)

    ax1.set_xlabel('percent lift')
    ax1.set_ylabel('probability of choosing A')
    ax1.legend(loc=4)

    ax2.set_xlim([lifts[0], lifts[-1]])
    ax2.plot(lifts, avg_run_times_lower, label='n = %d'% n1)
    ax2.plot(lifts, avg_run_times_mean, label='n = %d'% n2)
    ax2.plot(lifts, avg_run_times_upper, label='n = %d' % n3)

    ax2.set_xlabel('percent lift')
    ax2.set_ylabel('impressions per banner')
    ax2.legend()
    plt.show()
    if fig_name:
        fig.savefig(fig_name)

