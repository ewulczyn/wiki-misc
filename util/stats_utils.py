
from scipy.stats import norm
from numpy.random import  beta
import numpy as np
from statsmodels.stats.power import tt_ind_solve_power



def samples_per_branch_calculator(u_hat, mde=0.05, alpha=0.05, power=0.95):
    var_hat = u_hat*(1-u_hat)
    standardized_effect_size =  (u_hat - (u_hat*(1+mde))) / np.sqrt(var_hat)
    sample_size = tt_ind_solve_power(effect_size=standardized_effect_size, alpha=alpha, power=power)
    return sample_size



def remove_outliers(d):
    """takes in a padas series of real numbers and removes outlier"""
    return d[np.abs(d-d.mean()) <= (3*d.std())]
   

def difference_in_means_confidence_interval(A_donation_amounts, A_num_events, B_donation_amounts, B_num_events, alpha = 0.05):

    """


    Args:
        A_donation_amounts, B_donation_amounts : pandas.Series of dollar donations
        A_num_events, B_num_events: int , usually either number of clicks or impressions


    Keyword arguments:
        alpha -- significance level

    Returns:

    """

    #need special variance function, since we are representing 0 values as a count
    def var(u, n, counts):
        sss = 0.0
        for index, value in counts.iteritems():
            sss+= value*(u-index)**2
        return sss / n


    xbar1 = A_donation_amounts.sum() / A_num_events
    xbar2 = B_donation_amounts.sum() / B_num_events

    n1 = A_num_events-1
    n2 = B_num_events-1

    counts1 = A_donation_amounts.value_counts()
    counts1.set_value(0, A_num_events - A_donation_amounts.shape[0])
    counts2 = B_donation_amounts.value_counts()
    counts2.set_value(0, B_num_events - B_donation_amounts.shape[0])

    sigma1 = var(xbar1, n1, counts1)
    sigma2 = var(xbar2, n2, counts2)

    se = np.sqrt(sigma1*sigma1/n1 + sigma2*sigma2/n2)
    zcrit = norm.ppf(1.0- alpha/2.0)
    error = zcrit * se
    return (float(xbar1 - xbar2 - error), float(xbar1 -xbar2+ error))


def get_beta_dist(num_events, num_trials):
    return beta(num_events+1, num_trials-num_events+1, 50000)


def bayesian_ci(dist, conf):
    return (np.percentile(dist, (100.0 - conf)/2.0 ), np.percentile(dist, conf + (100.0 - conf)/2.0 ))

    