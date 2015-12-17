from statsmodels.stats.power import tt_ind_solve_power
import numpy as np
from scipy.stats import ttest_ind
from scipy.special import stdtr
from abstract_abtest import ABTest


class NHTEstimator:
    """
    Maintains and exposes information about a banner
    that is necessary to run perform a ttest
    """
    def __init__(self, values):
        self.values = values
        self.counts = np.zeros(values.shape, dtype='int64')
        self.N = 0

    def update(self, counts):
        self.counts += counts
        self.N = self.counts.sum()

    def mean(self):
        return float(self.values.dot(self.counts)) / np.sum(self.counts)
    def num_samples(self):
        return float(np.sum(self.counts))

    def var(self):
        u = self.mean()
        n = self.num_samples()
        return sum([self.counts[i] * (u - self.values[i]) ** 2 for i in range(len(self.values))]) / n




class NHABTest(ABTest):
    """
    This AB Test class uses null hypothesis tests as a stopping criterion
    """
    def __init__(self, a_stream, b_stream, test_interval, max_run, alpha):

        super(NHABTest, self).__init__(a_stream, b_stream, test_interval, max_run)

        self.alpha = alpha
        self.a_estimator = NHTEstimator(a_stream.p.values)
        self.b_estimator = NHTEstimator(b_stream.p.values)


    def evaluate_stopping_criterium(self):
        p = self.ttest()
        if p < self.alpha:
            if np.mean(self.a_estimator.mean()) > np.mean(self.b_estimator.mean()):
                return "A"
            else:
                return "B"
        if self.max_run < max(self.a_estimator.N, self.b_estimator.N):
            return 'unknown'
        else:
            return 'continue'


    def ttest(self):
        """
        ttest implementation that uses efficient variance computation
        """
        abar = self.a_estimator.mean()
        bbar = self.b_estimator.mean()

        na = self.a_estimator.num_samples()
        adof = na - 1
        nb = self.b_estimator.num_samples()
        bdof = nb - 1

        avar = self.a_estimator.var()
        bvar = self.b_estimator.var()

        tf = (abar - bbar) / np.sqrt(avar/na + bvar/nb)
        dof = (avar / na + bvar / nb) ** 2 / (avar ** 2 / (na ** 2 * adof) + bvar ** 2 / (nb ** 2 * bdof))
        pf = 2*stdtr(dof, -np.abs(tf))
        return pf

def samples_per_branch_calculator(p_hat, mde, alpha, power):
    """
    Classical sample size calculation for ttest
    """
    u_hat = p_hat.values.dot(p_hat.distribution)
    sigma_hat = sum([p_hat.distribution[i] * ((p_hat.values[i] - u_hat) ** 2) for i in range(len(p_hat.distribution))])
    standardized_effect_size = (u_hat - (u_hat * (1 + mde))) / np.sqrt(sigma_hat)
    sample_size = tt_ind_solve_power(effect_size=standardized_effect_size, alpha=alpha, power=power)
    return sample_size




    