from numpy.random import beta as beta_dist
from numpy import mean, concatenate, zeros
from bayesian_log_normal import draw_log_normal_means;
from numpy.random import lognormal




def bayesian_log_normal_with_0_test (A_data, B_data, m0, k0,s_sq0, v0, mean_lift = 0):

    # modeling zero vs. non-zero
    non_zeros_A = sum(A_data > 0)
    total_A = len(A_data)
    non_zeros_B = sum(B_data > 0)
    total_B = len(B_data)
    alpha = 1 # uniform prior
    beta = 1

    n_samples = 10000 # number of samples to draw
    A_conv_samps = beta_dist(non_zeros_A+alpha, total_A-non_zeros_A+beta, n_samples)
    B_conv_samps = beta_dist(non_zeros_B+alpha, total_B-non_zeros_B+beta, n_samples)

    # modeling the non-zeros with a log-normal
    A_non_zero_data = A_data[A_data > 0]
    B_non_zero_data = B_data[B_data > 0]

    A_order_samps = draw_log_normal_means(A_non_zero_data,m0,k0,s_sq0,v0)
    B_order_samps = draw_log_normal_means(B_non_zero_data,m0,k0,s_sq0,v0)

    # combining the two
    A_rps_samps = A_conv_samps*A_order_samps
    B_rps_samps = B_conv_samps*B_order_samps

    # the result
    print mean(A_rps_samps > B_rps_samps)


# some random log-normal data
A_log_norm_data = lognormal(mean=4.10, sigma=1.0, size=100)
B_log_norm_data = lognormal(mean=4.00, sigma=1.0, size=100)
# appending many many zeros
A_data = concatenate([A_log_norm_data,zeros((10000))])
B_data = concatenate([B_log_norm_data,zeros((10000))])

m0 = 4.
k0 = 1.
s_sq0 = 1.
v0 = 1.

bayesian_log_normal_with_0_test (A_data, B_data, m0, k0,s_sq0, v0)
