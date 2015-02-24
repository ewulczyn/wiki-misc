

    from  scipy.stats import binom
    import matplotlib.pyplot as plt
    %matplotlib inline
    import numpy as np
    import math 
    import pandas as pd
    pd.options.display.mpl_style = 'default'
    


$$
P(N=j | n) = \frac{P(n | N=j)*P(N=j)}{\sum\limits_{i=n}^N{P(n|N=i)*P(N=i)}}
$$

#AB Testing with Sampled Data

The Wikimedia Foundation (WMF) is in the unique position that we have a ton of
traffic and very little resources. When AB testing banners, we get so many
impressions we have to sample the number of impressions we record. This
complicates estimating the conversion rate for our banners. In traditional AB
testing, we model the conversion rate as a bernoulli random variable. When
running an AB test with unsampled impression counts, all the uncertainty in true
conversion rates comes from the fact that a conversion is a random event and we
have a finite number of impressions. However, if we sample the number of
impressions, we also need to deal with the uncertainty in the number of banners
we actually served! What follows is an extension the bayesian hypothesis test
for bernoulli data that accounts for the additonal uncertainty introduced by
sampling.


##Why do we sample impressions?

Before I dig into the theory, let me explain exactly what I mean when I say the
impression numbers are sampled and why they are sampled. During a fundraising
campaign, we try to serve a banner on every pageview. When the page loads, it
fetches the current banner for the experimental condition the client is in.
Depending on client side cookies set by previous fundraising banners, the banner
may or may not display. For example, if the client closed a banner in the last
two weeks, the banner may choose to hide itself. Recording whether there was an
impression requires sending an extra request back to our servers. The operations
team deems it unacceptable to send such an banner impression logging request for
every pageview. So instead, we send the request back with probability r (0.01).

At this point I assume you are scratching your head. Why can't we just decide if
the banner will be displayed server side and avoid sending the request. Well,
the WMF infrastructure is designed to serve requests for non logged-in users out
of cache, even banners. As such it is not possible to evaluate if the banner
should be shown based on the client's fundraising cookies server-side. I'm sure
there are ways to get around sampling, but the fact that impressons are sampled
is a constraint I currenlty face.

##Estimating the number of Impressions

The first challenge in developing theory for AB testing under these
circumstances is to estimate how many impressions there really were (N) given
the number of logged impression records (n). To make this formal, consider a
random process in which we randomly record an event with fixed, known
probability $r$. Say we have $n$ recorded events. Lets work out $P(N | n)$, the
distribution over the true number of events $N$, given the sampling rate $r$ and
the number of recorded events $n$:

Note: If you remember your discrete distributions, this might sound reminiscent
of a negative binomial random variable. That is almost correct. The negative
binomial random variable corresponds to the number of attempts (N) until the nth
success. In our scenario, we don't know how many impressions there where after
the last recorded impression (success). What follows is the derivation of the
correct distribution. For situations where you have so much data that you have
to sample, it turns out to be very similar to the negative binomial. So if you
don't enjoy proofs, feel free to skip to the next section.

We know that $P(n|N)$ ~  Binomial(N, r). Applying Bayes rule, we get:

$$ P(N=j |n) = \frac{P(n | N=j)*P(N=j)}{\sum\limits_{i=n}^N{P(n|N=i)*P(N=i)}}$$

If we assume a uniform prior over $N$ (this is a big one),

$$ P(N=j |n) = \frac{P(n | N=j)}{\sum\limits_{i=n}^\infty{P(n|N=i)}} = \frac{{j
\choose n} r^{n} (1-r)^{j-n}}{\sum\limits_{i=n}^\infty{{i \choose n} r^{n}
(1-r)^{i-n}}} $$

Lets work out the infinite sum in the denominator:

$$
\sum\limits_{i=n}^\infty{{i \choose n} r^{n} (1-r)^{i-n}} =
\sum\limits_{i^\prime=n^\prime}^\infty{{i^\prime -1 \choose n^\prime -1}
r^{n^\prime -1} (1-r)^{i^\prime-n^\prime}} =
\frac{1}{r} * \sum\limits_{i^\prime=n^\prime}^\infty{{i^\prime -1 \choose
n^\prime -1} r^{n^\prime} (1-r)^{i^\prime-n^\prime}} = \frac{1}{r}
$$


In step 1, we make a transformation of variables. We set $n = n^\prime -1$ and
$i = i^\prime -1$. In step 2 we make use of the fact that ${i^\prime -1 \choose
n^\prime -1} r^{n^\prime} (1-r)^{i^\prime-n^\prime}$ is the pmf of the negative
binomial distribution.

So, we arrive at our final result:
$$
P(N | r, n) = \begin{cases}
0, &{\rm if}\ N\ \text{less than}\ n  \\
{N \choose n} r^{n+1} (1-r)^{N-n}, &{\rm otherwise}
\end{cases}
$$


Next, lets prove that this is a real distribution:
$$
\sum\limits_{i=n}^\infty{{i \choose n} r^{n+1} (1-r)^{i-n}}=
r * \sum\limits_{i=n}^\infty{{i \choose n} r^{n} (1-r)^{i-n}} = r * \frac{1}{r}
= 1
$$

In step 1, we make use of the above proof showing $\sum\limits_{i=n}^\infty{{i
\choose n} r^{n} (1-r)^{i-n}} = \frac{1}{r}$

Lets refer to this newly derived distribution as the "inverse binomial
distribution". Next, lets work out the expectation:

$$
E[N] =
\sum\limits_{i=n}^\infty{i*{i \choose n} r^{n+1} (1-r)^{i-n}} =
$$
$$
\sum\limits_{i^\prime=n^\prime}^\infty{(i^\prime -1)*{i^\prime -1 \choose
n^\prime -1} r^{n^\prime} (1-r)^{i^\prime-n^\prime}} =
$$
$$
\sum\limits_{i^\prime=n^\prime}^\infty{i^\prime * {i^\prime -1 \choose n^\prime
-1} r^{n^\prime} (1-r)^{i^\prime-n^\prime}} -
\sum\limits_{i^\prime=n^\prime}^\infty{{i^\prime -1 \choose n^\prime -1}
r^{n^\prime} (1-r)^{i^\prime-n^\prime}}=
$$
$$
\frac{n^\prime}{r} - 1 =
$$
$$
\frac{n + 1}{r} -1
$$

What a nice result! In step 3 we make use of the fact that
$\sum\limits_{i^\prime=n^\prime}^\infty{i^\prime * {i^\prime -1 \choose n^\prime
-1} r^{n^\prime} (1-r)^{i^\prime-n^\prime}}$ is the expectation of a negative
binomial distribution.


    # simple implementation of n choose k
    def nCk(n,k):
        f = math.factorial
        return f(n) / f(k) / f(n-k)
    
    # probability mass function for the Number of original data points, given the sampling rate and the number
    # of observed points
    def inverse_binom(N, n, r):
        return nCk(N, n)*(r**(n+1))*((1-r)**(N-n))
    
    def compute_expectation(n, r):
        return (n+1)/r -1
    
    
    def is_prob(n, r, limit):
        ex = 0.0
        for i in range(n, limit):
            ex += inverse_binom(i, n, r)
        return ex



    # Just so you believe me that the inverse binomial is a real distribution
    print is_prob(100, 0.1, 2000)

    1.0


Lets look at the expected number of points as a function of the sampling rate,
given that we observe 1 example:



    def compare_negative_to_inverse(n):
        rs = np.arange(0.1, 0.99, 0.1)
        expectations = [compute_expectation(n, r) for r in rs]
        plt.plot(rs, expectations, label='inverse binomial (n, r)')
        plt.plot(rs, [n/i for i in rs], label = 'negative binomial (n, r)' )
        plt.plot(rs, [(n+1)/i for i in rs], label='negative binomial (n+1, r)' )
        plt.legend()
        plt.xlabel('samplig rate: r')
        plt.ylabel('expectation')


    import numpy as np
    import matplotlib.pyplot as plt
    
    def negative_binomial_expectation(n, r):
        return n/r
    
    def inverse_binomial_expectation(n, r):
        return (n+1)/r -1
    
    def compare_expectations(n):
        fig = plt.figure(figsize =(10, 6))
        rs = np.arange(0.1, 0.99, 0.1)
        inv_binom = [inverse_binomial_expectation(n, r) for r in rs]
        neg_binom_lower = [negative_binomial_expectation(n, r) for r in rs]
        neg_binom_upper = [negative_binomial_expectation(n+1, r) for r in rs]
        plt.plot(rs, inv_binom, label='inverse binomial n=%d' % n)
        plt.plot(rs, neg_binom_lower, label = 'negative binomial n=%d' % n )
        plt.plot(rs,neg_binom_upper, label='negative binomial n=%d' % (n+1))
        plt.legend()
        plt.xlabel('samplig rate: r')
        plt.ylabel('expectation')
        plt.show()
    
    compare_expectations(1)


![png](AB_testing_with_sampled_data_files/AB_testing_with_sampled_data_16_0.png)



    compare_negative_to_inverse(1)


![png](AB_testing_with_sampled_data_files/AB_testing_with_sampled_data_17_0.png)


As mentioned above, the negative binomial random variable is the expected number
of coin tosses before the nth heads. This is pretty close to our inverse
binomial random variable, the difference being that we are not guaranteed that
the last of our data points was sampled. There could be arbitrarily many data
points that we did not sample after the last one we sampled. Hence we would
expect the expectation of the negative binomial distribution to be a lower bound
for the inverse binomial distribution. Furthermore the the expectation of the
negative binomial distribution, where we act as if we saw one more sample than
we really did, is an upper bound for our inverse binomial rv.

Depending on r the inverse binomial, swings between it supper and lower bounds.
Lets see what happens for larger values on n.


    compare_negative_to_inverse(10)


![png](AB_testing_with_sampled_data_files/AB_testing_with_sampled_data_19_0.png)


If n increase, the difference in expectation between the distributions becomes
less and less significant. Hence, for fundraising AB tests, we may as well
assume that our impressions is negative binomial.

## The Traditional Bayesian AB Test

In traditional bayesian AB testing, we model a conversion as bernoulli random
variable. We also model the conversion rate p as a random variable following a
beta distribution. Before observing any data, we model p as $beta(a_{prior},
b_{prior})$. Usually, we set $a_{prior}=b_{prior}=1$, which corresponds to a
uniform distribution over the interval $[0,1]$. After running a banner we count
how many impressions led to a donation (successes) and how many impressions did
not (failures).


Then we update the distribution over p to be  $beta(a_{prior} + successes,
b_{prior} + failures)$. Once we have the distributions for the conversion rate
for each of the banners, we can use numerical methods to arrive at the
distribution over functions of the conversion rates. We will be most interested
in the distribution over the percent difference in conversion rates. For a more
thorough introduction to bayesian AB testing, check out
(http://engineering.richrelevance.com/bayesian-ab-tests/).

The code below demonstrates running a traditional bayesian AB test, where the
the number of impressions are known in advance.


    from numpy.random import binomial, beta
    
    # True donation rates
    p_A = 0.5
    p_B = 0.6 # B has a 20% lift over A
    
    
    # True number of impressions per banner
    N_A = 1000
    N_B = 1200
    
    #simulate Running the banner
    successes_A = binomial(N_A, p_A)
    successes_B = binomial(N_B, p_B)
    
    # form posterior distribution over the lift  banner B gives over banner A
    failures_A = N_A -successes_A
    failures_B = N_B -successes_B
    
    N_samples = 20000
    p_A_posterior = beta(1+successes_A, 1+failures_A, size = N_samples)
    p_B_posterior =  beta(1+successes_B, 1+failures_B, size = N_samples)
    
    lift_distribution = (p_B_posterior-p_A_posterior)/p_A_posterior
    lift_distribution = lift_distribution *100
    
    # 1-a Credible interval for the lift that B ahs over A
    a = 10
    lower = np.percentile(lift_distribution, a/2)
    upper = np.percentile(lift_distribution, 100- a/2)
    print "%d%% Confidence Interval for the lift B has over A: (%f, %f)" %(100-a, lower, upper)

    90% Confidence Interval for the lift B has over A: (6.510247, 21.777047)


## The Traditional Method Break Down with Sampled Data


The method described above won't work for us. When we sample the number of
impressions, we still know how many successes we got, since we process the
payment for every donation and the donation hits our accounts. But we do not how
many impressions did not lead to a donation (failures). We will have to find a
way of incorporate the uncertainty we have in the number of failed impressions
into the traditional method.

Lets start with a naive approach of simply using the expected number of failed
impression given the sampling rate r and the number of observed failed
impressions n: $\frac{(n+1)}{r} - 1$.


Note: How do we get the number of failed impressions when all the have is the
number of observed impressions? We will need to generate an ID on each
impression and log that ID with each impression and each donation. Then we can
get the number of observed impressions that did not lead to a donation by
removing impressions with the same ID as a donation.

Lets see how this naive approach behaves. We will be interested in measuring how
often our 100-a% confidence intervals for the lift that banner B has over banner
A cover the true lift.


    from numpy.random import negative_binomial, binomial, beta
    
    
    def get_p_dist_naive(successes, observed_failures, r):
        """
        get posterior distribution over the conversion
        rate p, when using the expected number of failed impressions
        given a sampling rate of r and n observed failures
        """
        estimated_failures = int(compute_expectation(observed_failures, r))
        return beta(1+successes, 1+estimated_failures, size = 20000)
    
    
    def get_lift_confidence_interval(p_A_posterior, p_B_posterior, conf):
        """
        compute a credible interval over the lift that
        banner A has over banner B given the data
        """
        a = (100-conf) 
        lift_distribution = (p_B_posterior-p_A_posterior)/p_A_posterior
        lift_distribution = lift_distribution *100
        lower = np.percentile(lift_distribution, a/2)
        upper = np.percentile(lift_distribution, 100- a/2)
        return (lower, upper)
    
    def lift_interval_covers_true_lift(interval,true_lift):
        """
        Check if the credible interval covers the true parameter
        """
        if interval[1] < true_lift:
            return 0
        if interval[0] > true_lift:
            return 0
        return 1
            
    
        
    
    def measure_coverage(N, p_A, lift, r, get_p_dist, iters = 500, conf = 90, ):
        """
        This function simulates iters AB tests and measures the coverage of the 
        confidence interval
        """
        p_B = p_A * ((100.+lift)/100.)
    
        interval_covers = 0.0
    
    
        for i in range(iters):
            successes_A = binomial(N, p_A)
            observed_failures_A = binomial(N-successes_A, r)
            p_A_posterior = get_p_dist(successes_A, observed_failures_A, r)
    
    
            successes_B = binomial(N, p_B)
            observed_failures_B = binomial(N-successes_B, r)
            p_B_posterior = get_p_dist(successes_B, observed_failures_B, r)
    
            interval = get_lift_confidence_interval(p_A_posterior, p_B_posterior, conf)
            interval_covers += lift_interval_covers_true_lift(interval, lift)
        coverage = (interval_covers/iters)*100
        print "%d %% lift interval covers true lift covers %f%% of the time" % (conf, coverage)
        return coverage
     

Lets see what happens when we don't sample. This is equivalent to the
traditional method


    N = 1000000
    p_A = 0.008
    lift = 3.
    rs = [1.0, 1/10.,1/50., 1/100.,1/500., 1/1000.]
    naive_coverages = [measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000) for r in rs]


    90 % lift interval covers true lift covers 89.100000% of the time
    90 % lift interval covers true lift covers 89.600000% of the time
    90 % lift interval covers true lift covers 84.500000% of the time
    90 % lift interval covers true lift covers 76.500000% of the time
    90 % lift interval covers true lift covers 53.900000% of the time
    90 % lift interval covers true lift covers 43.600000% of the time



    x = range(len(rs))
    
    fig = plt.figure(figsize =(10, 6))
    plt.plot(x, naive_coverages)
    plt.xlabel('samplig rate: r')
    plt.xticks (range(len(x)), ["1", "1/10", "1/50", "1/100", "1/500", "1/1000"])
    plt.ylabel('percent coverage')
    plt.title("Coverage of 90% CIs as a function of sampling rate")
    plt.ylim(0, 100)
    plt.show()


![png](AB_testing_with_sampled_data_files/AB_testing_with_sampled_data_30_0.png)



    proper_coverages = [measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000) for r in rs]


    90 % lift interval covers true lift covers 90.200000% of the time
    90 % lift interval covers true lift covers 87.900000% of the time
    90 % lift interval covers true lift covers 90.300000% of the time
    90 % lift interval covers true lift covers 89.600000% of the time
    90 % lift interval covers true lift covers 89.800000% of the time
    90 % lift interval covers true lift covers 91.400000% of the time



    x = range(len(rs))
    
    fig = plt.figure(figsize =(10, 6))
    plt.plot(x, proper_coverages)
    plt.xlabel('samplig rate: r')
    plt.xticks (range(len(x)), ["1", "1/10", "1/50", "1/100", "1/500", "1/1000"])
    plt.ylabel('percent coverage')
    plt.title("Coverage of 90% CIs as a function of sampling rate")
    plt.ylim(0, 100)
    plt.show()



![png](AB_testing_with_sampled_data_files/AB_testing_with_sampled_data_32_0.png)


Great, we expect the traditonal method to give good confidence intervals. Lets
wee what happens as we start sampling more heavily


    r = 1/10.
    measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000)


    90 % lift interval covers true lift covers 88.600000% of the time



    r = 1/50.
    measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000)


    90 % lift interval covers true lift covers 84.700000% of the time



    r = 1/100.
    measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000)


    90 % lift interval covers true lift covers 76.600000% of the time



    r = 1/500.
    measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000)


    90 % lift interval covers true lift covers 51.800000% of the time



    r = 1/1000.
    measure_coverage(N, p_A, lift, r, get_p_dist_naive, iters = 1000)


    90 % lift interval covers true lift covers 42.300000% of the time


We see the traditonal method gets worse and worse as we increase the sampling
rate. Lets incormorate the uncertainty we have in the number of failed
impressions, first sampling from the inverse binomial to get the number of
failed impressions and them sampling from the beta:


    def get_p_dist_proper(successes, observed_failures, r):
        """
        get posterior distribution over the conversion
        rate p, in stead of using a fixed estimate
        for the number of failed impressions, given a sampling rate of r and n observed failures
        lets sample from a negative binomial
        (this should be an inverse binomial)
        """
        missing_failures_distribution = negative_binomial(observed_failures, r, size = 20000)
        failures_distribution = missing_failures_distribution + observed_failures
        def sample(num_failures):
            return beta(1+successes, 1+num_failures)
        p_hat = map(sample, failures_distribution)
        return np.array(p_hat)


    N = 1000000
    p_A = 0.008
    lift = 3.
    r = 1 / 1.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)

    90 % lift interval covers true lift covers 90.600000% of the time


Sanity Check Passed: Without sampling this method reduces to the traditional
test.



    r = 1/10.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)


    90 % lift interval covers true lift covers 91.100000% of the time



    r = 1/50.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)
    


    90 % lift interval covers true lift covers 89.800000% of the time



    r = 1/100.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)


    90 % lift interval covers true lift covers 90.400000% of the time



    r = 1/500.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)


    90 % lift interval covers true lift covers 91.100000% of the time



    r = 1/1000.
    measure_coverage(N, p_A, lift, r, get_p_dist_proper, iters = 1000)


    90 % lift interval covers true lift covers 88.700000% of the time


Next steps:
ß- figure out how much longer we need to run our average test as a function of
sampling rate


    def prob_A_is_better(dA, dB):
        return (dA > dB).mean()


    
