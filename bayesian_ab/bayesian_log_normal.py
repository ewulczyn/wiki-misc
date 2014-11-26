from numpy import  mean
from numpy import exp, log
from bayesian_normal import draw_mus_and_sigmas
from numpy.random import lognormal




def draw_log_normal_means(data,m0,k0,s_sq0,v0,n_samples=10000):
    # log transform the data
    log_data = log(data)
    # get samples from the posterior
    mu_samples, sig_sq_samples = draw_mus_and_sigmas(log_data,m0,k0,s_sq0,v0,n_samples)
    # transform into log-normal means
    log_normal_mean_samples = exp(mu_samples + sig_sq_samples/2)
    return log_normal_mean_samples





def bayesian_log_normal_test (A_data, B_data, m0, k0,s_sq0, v0, mean_lift = 0, std_lift = 0):
    # step 1: get posterior samples
    A_posterior_samples = draw_log_normal_means(A_data,m0,k0,s_sq0,v0)
    B_posterior_samples = draw_log_normal_means(B_data,m0,k0,s_sq0,v0)

    # step 2: perform numerical integration
    prob_A_greater_B = mean(A_posterior_samples > B_posterior_samples)
    print prob_A_greater_B




# step 1: define prior parameters for inverse gamma and the normal
m0 = 4. # guess about the log of the mean
k0 = 1. # certainty about m.  compare with number of data samples
s_sq0 = 1. # degrees of freedom of sigma squared parameter
v0 = 1. # scale of sigma_squared parameter

# step 2: get some random data, with slightly different means
A_data = lognormal(mean=4.05, sigma=1.0, size=500)
B_data = lognormal(mean=4.00,  sigma=1.0, size=500)

bayesian_log_normal_test (A_data, B_data, m0, k0,s_sq0, v0)

