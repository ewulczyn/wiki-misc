from numpy import sum, mean, size, sqrt
from scipy.stats import norm, invgamma
from numpy.random import normal


def draw_mus_and_sigmas(data, m0, k0, s_sq0, v0,n_samples=10000):
    # number of samples
    N = size(data)
    # find the mean of the data
    the_mean = mean(data)
    # sum of squared differences between data and mean
    SSD = sum( (data - the_mean)**2 )

    # combining the prior with the data - page 79 of Gelman et al.
    # to make sense of this note that
    # inv-chi-sq(v,s^2) = inv-gamma(v/2,(v*s^2)/2)
    kN = float(k0 + N)
    mN = (k0/kN)*m0 + (N/kN)*the_mean
    vN = v0 + N
    vN_times_s_sqN = v0*s_sq0 + SSD + (N*k0*(m0-the_mean)**2)/kN

    # 1) draw the variances from an inverse gamma
    # (params: alpha, beta)
    alpha = vN/2
    beta = vN_times_s_sqN/2
    # thanks to wikipedia, we know that:
    # if X ~ inv-gamma(a,1) then b*X ~ inv-gamma(a,b)
    sig_sq_samples = beta*invgamma.rvs(alpha,size=n_samples)

    # 2) draw means from a normal conditioned on the drawn sigmas
    # (params: mean_norm, var_norm)
    mean_norm = mN
    var_norm = sqrt(sig_sq_samples/kN)
    mu_samples = norm.rvs(mean_norm,scale=var_norm,size=n_samples)

    # 3) return the mu_samples and sig_sq_samples
    return mu_samples, sig_sq_samples


"""

    m0 - Guess about where the mean is.
    k0 - Certainty about m0.  Compare with number of data samples.
    s_sq0 - Number of degrees of freedom of variance.
    v0 - Scale of the sigma_squared parameter.  Compare with number of data samples.

"""


def bayesian_normal_test (A_data, B_data, m0, k0,s_sq0, v0, mean_lift = 0, std_lift = 0):

    # step 1: get posterior samples
    A_mus,A_sig_sqs = draw_mus_and_sigmas(A_data,m0,k0,s_sq0,v0)
    B_mus,B_sig_sqs = draw_mus_and_sigmas(B_data,m0,k0,s_sq0,v0)

    # step 2: perform numerical integration
    # probability that mean of A is greater than mean of B:
    p_mean =  mean(A_mus > B_mus)
    # probability that variance of A is greater than variance of B:
    p_std =  mean(A_sig_sqs > B_sig_sqs)
    return (p_mean, p_std)



# step 1: define prior parameters for the normal and inverse gamma
m0 = 4.
k0 = 1.
s_sq0 = 1.
v0 = 1.

# step 2: get some random data, with slightly different statistics
A_data = normal(loc=4.1, scale=0.9, size=500)
B_data = normal(loc=4.0, scale=1.0, size=500)

bayesian_normal_test (A_data, B_data,m0,k0,s_sq0,v0 )