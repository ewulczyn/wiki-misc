
from scipy.stats import norm
from numpy.random import  beta
import numpy as np
from statsmodels.stats.power import tt_ind_solve_power
from numpy.random import dirichlet
import pandas as pd
import matplotlib.pyplot as plt




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
        A_donation_amounts, B_donation_amounts : pandas.Series of amount frequencies

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


def custom_rate_stats(a_num_events, a_num_trials, b_num_events, b_num_trials, conf=95, plot =True):
    a_dist = get_beta_dist(a_num_events, a_num_trials)
    b_dist = get_beta_dist(b_num_events, b_num_trials)
    d = pd.DataFrame.from_dict({'A':a_dist, 'B':b_dist})
    return print_stats(d, conf, plot)


def print_stats(dists, conf, plot):

    """
    Helper function to create a pandas datframe with rate statistics
    """

    if plot:
        plot_dist(dists)
    result_df = pd.DataFrame()

    def f(d):
        rci = bayesian_ci(d, conf)
        return "(%0.6f, %0.6f)" % (rci[0], rci[1])

    result_df['CI'] = dists.apply(f)

    def f(d):
        return d.idxmax()
    best = dists.apply(f, axis=1)
    result_df['P(Winner)'] = best.value_counts() / best.shape[0]
    result_df = result_df.sort('P(Winner)', ascending=False)

    def f(d):
        ref_d = dists[result_df.index[0]]
        lift_ci = bayesian_ci(100.0 * ((ref_d - d) / d), conf)
        return "(%0.2f%%, %0.2f%%)" % (lift_ci[0], lift_ci[1])

    result_df['Winners Lift'] = dists.apply(f)

    return result_df[['P(Winner)', 'Winners Lift', 'CI']]

def plot_dist(dists):
    """
    Helper function to plot the probability distribution over
    the donation rates (bayesian formalism)
    """
    fig, ax = plt.subplots(1, 1, figsize=(13, 3))

    bins = 50
    for name in dists.columns:
        ax.hist(dists[name], bins=bins, alpha=0.6, label=name, normed=True)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    


def get_multinomial_expectation_dist(counts, alpha = None, num_samples = 50000):
    if not alpha:
        alpha = np.ones(counts.shape)
    return dirichlet(counts + alpha, num_samples).dot(np.array(counts.index).transpose())
    