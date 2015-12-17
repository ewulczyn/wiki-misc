__author__ = 'bholley'
import numpy as np
import pymc as pm
import scipy
import math
import matplotlib.pyplot as plt
#generate fake data
n=10000
p = 0.1  # non-zero entries
mu = 0
std = math.sqrt(0.7)

threshold = np.random.binomial(n, 1-p, size=1)[0]
zero_data = np.zeros(threshold)
log_normal_data =  np.random.lognormal(mu, std, n-threshold) # numpy normal takes std
print "mean of normal distribution" , np.log(log_normal_data).mean()
data = np.concatenate((zero_data,log_normal_data), axis=0)
np.random.shuffle(data)

plt.hist(data, 100)
plt.show()

_p = pm.Uniform("p", lower=0, upper=1)

_tau = 1.0 / pm.Uniform("std", 0.1, 1.5) ** 2
_mu = pm.Normal("mu", 0.1, 0.01)


#return log-likihood of sample
@pm.stochastic(observed=True)
def custom(value = data,_p=_p, _tau=_tau, _mu=_mu):
    ll=np.sum(value==0)*np.log(_p)
    ll+=np.sum(value>0)*np.log(1-_p)
    ll+=pm.distributions.lognormal_like(value[value>0], _mu, _tau) #pymc takes tau=1/var
    return ll


model = pm.Model([custom, _p, _tau, _mu,])
mcmc = pm.MCMC(model)
mcmc.sample(40000, 10000, 1)

p_samples = mcmc.trace("p")[:]
mu_samples = mcmc.trace("mu")[:]
std_samples = mcmc.trace("std")[:]


# histogram of posteriors

ax = plt.subplot(311)

plt.hist(p_samples, histtype='stepfilled', bins=25, alpha=0.85,
         label="posterior of $p$", color="#A60628", normed=True)
#plt.vlines(p, 0, 80, linestyle="--", label="true $p_A$ (unknown)")
plt.legend(loc="upper right")
plt.title("Posterior distributions of $p_A$, $p_B$, and delta unknowns")

ax = plt.subplot(312)

plt.hist(mu_samples, histtype='stepfilled', bins=25, alpha=0.85,
         label="posterior of $mu$", color="#467821", normed=True)
#plt.vlines(mu, 0, 80, linestyle="--", label="true $p_B$ (unknown)")
plt.legend(loc="upper right")

ax = plt.subplot(313)
plt.hist(std_samples, histtype='stepfilled', bins=30, alpha=0.85,
         label="posterior of std", color="#7A68A6", normed=True)
#plt.vlines(std, 0, 60, linestyle="--",
#         label="true delta (unknown)")
#plt.vlines(0, 0, 60, color="black", alpha=0.2)
plt.legend(loc="upper right");

plt.show()