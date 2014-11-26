from numpy.random import binomial, multinomial, beta, dirichlet
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint
import pandas as pd
from abtest_util import SimStream, DonationProb
from abstract_abtest import ABTest
from abc import ABCMeta, abstractmethod


"""
Computes the probability that A has lift over B

"""

class BayesianCTREstimator:
    def __init__(self, alpha = 1, beta = 1):
        self.alpha = alpha
        self.beta = beta
        self.ones = 0
        self.zeros = 0
        self.distributions = []

    def update(self, records):
        self.zeros += records[0]
        self.ones += np.sum(records) - records[0]
        dist =  beta(self.ones+self.alpha, self.zeros+self.beta, 10000)
        self.distributions.append(dist)


class BayesianAmountEstimator:
    def __init__(self, values, alpha = None):
        self.values = values
        self.alpha = alpha
        if alpha is None:
            self.alpha = np.ones(values.shape)
        self.counts = np.zeros(values.shape)
        self.distributions = []
        self.N = 0

    def update(self, counts):
        self.counts += counts
        dist = dirichlet(self.counts+self.alpha, 10000).dot(self.values.transpose())
        self.distributions.append(dist)
        self.N = self.counts.sum()



class BayesianABTest(ABTest):
    __metaclass__ = ABCMeta
    def __init__(self, a_stream, b_stream, test_interval, max_run):
        ABTest. __init__(self, a_stream, b_stream, test_interval, max_run)
        self.a_estimator = BayesianAmountEstimator(a_stream.p.values)
        self.b_estimator = BayesianAmountEstimator(b_stream.p.values)

    #@abstractmethod
    def evaluate_stopping_criterium(self):
        pass




class CredibilityABTest(BayesianABTest):
    def __init__(self, a_stream, b_stream, test_interval, max_run, conf):
        super(CredibilityABTest, self).__init__(a_stream, b_stream, test_interval, max_run)
        self.conf = conf
        

    


    def evaluate_stopping_criterium(self):
        A_dist = self.a_estimator.distributions[-1]
        B_dist = self.b_estimator.distributions[-1]
        p_A_better = (A_dist > B_dist).mean()

        if p_A_better > self.conf:
           return 'A'
        elif 1.0-p_A_better > self.conf:
            return 'B'
        elif self.max_run < max(self.a_estimator.N, self.b_estimator.N):
            return 'unknown'
        else:
            return 'continue' 


class CostABTest(BayesianABTest):
    def __init__(self, a_stream, b_stream, test_interval, max_run, cost):
        super(CostABTest, self).__init__(a_stream, b_stream, test_interval, max_run)
        self.cost = cost


    def evaluate_stopping_criterium(self):
        A_dist = self.a_estimator.distributions[-1]
        B_dist = self.b_estimator.distributions[-1]

        if A_dist.mean() > B_dist.mean():
            #if we choose A, what is the expected cost
            cost = pd.Series((B_dist-A_dist)/A_dist).apply(lambda x: max(x, 0)).mean()
            #print cost
            if  cost < self.cost:
                return 'A'
        else:
            cost = pd.Series((A_dist-B_dist)/B_dist).apply(lambda x: max(x, 0)).mean()
            #print cost
            if cost < self.cost:
                return 'B'

        if self.max_run < max(self.a_estimator.N, self.b_estimator.N):
            return 'unknown'
        else:
            return 'continue'



"""
   

    def show_stats(self):

        steps = len(self.A.estimator.distributions)
        lift_distributions = [(self.A.estimator.distributions[i] - self.B.estimator.distributions[i]) / self.B.estimator.distributions[i] for i in range(steps)]

        p_lift_0 = [np.mean(lift_distributions[i] > 0) for i in range(steps)]
        p_lift_5 = [np.mean(lift_distributions[i] > 0.05) for i in range(steps)]
        p_lift_10 = [np.mean(lift_distributions[i] > 0.10) for i in range(steps)]

        mean_lift = [np.mean(lift_distributions[i]) for i in range(steps)]
        lower_lift = [np.percentile(lift_distributions[i], 5) for i in range(steps)]
        upper_lift = [np.percentile(lift_distributions[i], 95) for i in range(steps)]

        mean_A = [np.mean(self.A.estimator.distributions[i]) for i in range(steps)]
        lower_A = [np.percentile(self.A.estimator.distributions[i], 5) for i in range(steps)]
        upper_A = [np.percentile(self.A.estimator.distributions[i], 95) for i in range(steps)]

        mean_B = [np.mean(self.B.estimator.distributions[i]) for i in range(steps)]
        lower_B = [np.percentile(self.B.estimator.distributions[i], 5) for i in range(steps)]
        upper_B = [np.percentile(self.B.estimator.distributions[i], 95) for i in range(steps)]

        T = self.A.stream.request_times

        fig = plt.figure()
        ax1 = fig.add_subplot(311)
        ax2 = fig.add_subplot(312)
        ax3 = fig.add_subplot(313)

    
        ax2.plot(T, mean_lift, label= 'expected lift')
        ax2.plot(T, [0.0]*len(T))
        ax2.fill_between(T, lower_lift, upper_lift, alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)
        ax2.set_xlabel('number of trials')
        ax2.set_ylabel(' % lift A over Ba')
        ax2.legend()

        
        ax1.plot(T, mean_A, label= 'A')
        ax1.fill_between(T, lower_A, upper_A, alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)

        ax1.plot(T, mean_B, label= 'B')
        ax1.fill_between(T, lower_B, upper_B, alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)

        ax1.set_xlabel('number of trials')
        ax1.set_ylabel('prob of donationg')
        ax1.legend()
        
        ax3.plot(T, p_lift_0, label = 'prob lift is > 0%')
        ax3.plot(T, p_lift_5, label = 'prob lift is > 5%')
        ax3.plot(T, p_lift_10, label = 'prob lift > 10 %')
        ax3.set_xlabel('number of trials')
        ax3.set_ylabel('prob')
        ax3.legend()
        ax3.plot(T, [0.95]*len(T))
        plt.show()


"""





