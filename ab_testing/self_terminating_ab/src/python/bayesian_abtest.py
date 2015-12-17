from numpy.random import dirichlet
import numpy as np
from abstract_abtest import ABTest


class BayesianAmountEstimator:
    """
    Maintains a posterior (dirichlet) distribution over
    the parameters of a multinomial distribution
    over donation amounts.
    """

    def __init__(self, values, alpha=None):
        self.values = values
        self.alpha = alpha
        if alpha is None:
            self.alpha = np.ones(values.shape)
        self.counts = np.zeros(values.shape)
        self.distributions = []
        self.N = 0

    def update(self, counts):
        self.counts += counts
        dist = dirichlet(self.counts + self.alpha, 10000).dot(self.values.transpose())
        self.distributions.append(dist)
        self.N = self.counts.sum()



class BayesianABTest(ABTest):
    def __init__(self, a_stream, b_stream, test_interval, max_run):
        ABTest. __init__(self, a_stream, b_stream, test_interval, max_run)
        self.a_estimator = BayesianAmountEstimator(a_stream.p.values)
        self.b_estimator = BayesianAmountEstimator(b_stream.p.values)

    def evaluate_stopping_criterium(self):
        pass




class CredibilityABTest(BayesianABTest):
    """
    This class uses P(CTR_A > CTR_B) as a stopping criterion
    """
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
    """
    This class uses E(max(CTR_A - CTR_B, 0)) as a stopping criterion
    """
    def __init__(self, a_stream, b_stream, test_interval, max_run, cost):
        super(CostABTest, self).__init__(a_stream, b_stream, test_interval, max_run)
        self.cost = cost


    def evaluate_stopping_criterium(self):
        A_dist = self.a_estimator.distributions[-1]
        B_dist = self.b_estimator.distributions[-1]

        if A_dist.mean() > B_dist.mean():
            #if we choose A, what is the expected cost
            cost = np.maximum((B_dist-A_dist), 0.0).mean()
            if  cost < self.cost:
                return 'A'
        else:
            #cost = pd.Series((A_dist-B_dist)/B_dist).apply(lambda x: max(x, 0)).mean()
            cost = np.maximum((A_dist-B_dist), 0.0).mean()
            if cost < self.cost:
                return 'B'

        if self.max_run < max(self.a_estimator.N, self.b_estimator.N):
            return 'unknown'
        else:
            return 'continue'
