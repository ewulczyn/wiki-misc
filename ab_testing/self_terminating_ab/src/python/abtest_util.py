import matplotlib.pyplot as plt
import numpy as np
from numpy.random import multinomial
from numpy.random import beta as beta_dist
from pprint import pprint
import pandas as pd
from statsmodels.stats.power import tt_ind_solve_power



class SimStream(object):
    """
    Encapsulates a simulated stream 
    of banner data
    """
    def __init__(self, p):
        self.p = p
        self.request_times = []
        self.N = 0

    def get_next_records(self, n):
        self.N += n
        self.request_times.append(self.N)
        counts = multinomial(n, self.p.distribution, size=1)[0]
        return counts




class DonationProb(object):
    def __init__(self, p_donate, pos_amounts=[1.0,], pos_amounts_distribution=[1.0,]):
        """
        Donations to Wikimedia come in discrete units due
        the radio boxes with suggested donation amounts.
        The vast majority of impressions do not lead to a donation.
        This object encapsulates a multinomial distribution that is 
        parameterized in a slightly unusual way. You specify the 
        positive values, a distribution over positive values
        and the donation rate.
        """
        self.p_donate = p_donate
        self.pos_amounts = pos_amounts
        self.pos_amounts_distribution = pos_amounts_distribution
        zero = np.array([0,])
        p_donate = np.array([p_donate],)
        pos_amounts = np.array(pos_amounts)
        pos_amounts_distribution = np.array(pos_amounts_distribution)
        self.values = np.concatenate([zero,pos_amounts])
        self.distribution = np.concatenate([1 - p_donate, p_donate * pos_amounts_distribution])

    def lift(self, lift):
        """
        Returns a DonationProb object
        whose donation rate was increased by lift percent
        """
        p_donate = self.p_donate + self.p_donate * lift
        return DonationProb(p_donate, self.pos_amounts, self.pos_amounts_distribution)

    def change_p_donate(self, p_donate):
        """
        Returns a DonationProb object
        whose donation rate was changed to p_donate
        but the positive values and the distribution over 
        positive values is unchanged
        """
        return DonationProb(p_donate, self.pos_amounts, self.pos_amounts_distribution)


class EmpiricalDonationProb(DonationProb):
    """
    Given donation data, builds a DonationProb
    object that captures the empirical (multinomial)
    distribution over donation amounts
    """
    def __init__(self, counts, values):
        
        N = float(np.sum(counts))
        p_donate = 1.0 - counts[0] / N
        pos_amounts = values[1:]
        pos_amounts_distribution =  (counts[1:] / N) / p_donate
        DonationProb.__init__(self, p_donate, pos_amounts, pos_amounts_distribution)
        self.counts = counts

    def p_donate_ci(self, a=5, alpha =1, beta=1):
        """
        Rretuns a 100-a credible interval
        for the donation rate
        """
        ones = self.counts[1:]
        zeros = self.counts[0]
        dist = beta_dist(ones + alpha, zeros + beta, 10000)
        lower_bound = np.percentile(dist, a / 2.0)
        upper_bound = np.percentile(dist, 100 - a / 2.0)
        mean = np.mean(dist)
        return (lower_bound, self.p_donate, upper_bound)












