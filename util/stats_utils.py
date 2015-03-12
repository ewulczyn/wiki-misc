
from scipy.stats import norm
from numpy.random import  beta
import numpy as np
from statsmodels.stats.power import tt_ind_solve_power
from numpy.random import dirichlet



def samples_per_branch_calculator(u_hat, mde=0.05, alpha=0.05, power=0.95):
    var_hat = u_hat*(1-u_hat)
    standardized_effect_size =  (u_hat - (u_hat*(1+mde))) / np.sqrt(var_hat)
    sample_size = tt_ind_solve_power(effect_size=standardized_effect_size, alpha=alpha, power=power)
    return sample_size



def remove_outliers(d):
    """takes in a padas series of real numbers and removes outlier"""
    return d[np.abs(d-d.mean()) <= (3*d.std())]
   

def classic_difference_in_means_ci(a_value_counts, b_value_counts, alpha = 0.05):
    """
    Args:
        A_donation_amounts, B_donation_amounts : pandas.Series of amount frquencies

    Keyword arguments:
        alpha -- significance level
    """

    def var(u, value_counts):
        sss = 0.0
        for value, count in value_counts.iteritems():
            sss+= count*((u-value)**2)
        return sss / value_counts.sum()
    
    def mean(value_counts):
        values = np.array(value_counts.index)
        n = float(value_counts.sum())
        total = (values * value_counts).sum()
        return total / n
        
        
    u_a =  mean(a_value_counts)
    u_b =  mean(b_value_counts)

    n_a = a_value_counts.sum()-1
    n_b = b_value_counts.sum()-1

    var_a = var(u_a, a_value_counts)
    var_b = var(u_b, b_value_counts)

    pooled_se = np.sqrt(var_a/n_a + var_b/n_b)
    zcrit = norm.ppf(1.0- alpha/2.0)
    error = zcrit * pooled_se
    return (u_b - u_a - error), (u_b - u_a+ error)


def get_beta_dist(num_events, num_trials, num_samples = 50000):
    return beta(num_events+1, num_trials-num_events+1, num_samples)


def bayesian_ci(dist, conf):
    return (np.percentile(dist, (100.0 - conf)/2.0 ), np.percentile(dist, conf + (100.0 - conf)/2.0 ))


def get_multinomial_expectation_dist(counts, alpha = None, num_samples = 50000):
    if not alpha:
        alpha = np.ones(counts.shape)
    return dirichlet(counts + alpha, num_samples).dot(np.array(counts.index).transpose())
    