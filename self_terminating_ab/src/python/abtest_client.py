
from abtest_util import SimStream, Banner, DonationProb, expected_results, estimateDonationProb, EmpiricalDonationProb
from bayesian_abtest import BayesianABTest, BayesianAmountEstimator
from nh_abtest import NHABTest, NHTEstimator


"""
def probability_B_beats_A(α_A, β_A, α_B, β_B)
    total = 0.0
    for i = 0:(α_B-1)
        total += exp(lbeta(α_A+i, β_B+β_A) 
            - log(β_B+i) - lbeta(1+i, β_B) - lbeta(α_A, β_A))
    end
    return total
end
"""


if __name__ == '__main__':

    #p_B = DonationProb(0.0009, [3, 5, 10, 50], [0.6, 0.2, 0.15, 0.05,])
    #p_hat_B = p_B
    #p_A = DonationProb(0.0008, [3, 5, 10, 50], [0.6, 0.2, 0.15, 0.05,])
    #ctr = False

    p_B = DonationProb(0.0011, [1,], [1.0,])
    p_A = DonationProb(0.001, [1,], [1.0,])
    ctr = True

    
    hist_data_B = SimStream(p_B)
    p_hat = EmpiricalDonationProb(hist_data_B.get_next_records(50000), hist_data_B.p.values)
    ci = p_hat.p_donate_ci(10)
    print "CI: ", ci

    mde = 0.05
    max_run = float('inf')
    interval = round(1/ci[0])*100



    conf = 0.95
    
    B = Banner(SimStream(p_B), BayesianAmountEstimator(p_B.values))
    A = Banner(SimStream(p_A), BayesianAmountEstimator(p_A.values))
    t_bayes = BayesianABTest(A, B, interval, p_hat, mde, conf, max_run =max_run)


    alpha = 0.1
    power = 0.95 #only affects max run time


    B = Banner(SimStream(p_B), NHTEstimator(p_B.values))
    A = Banner(SimStream(p_A), NHTEstimator(p_A.values))
    t_nh = NHABTest(A, B, interval, p_hat, mde, alpha, power, max_run = max_run)


    lifts = [x/100.0 for x in range(-20, 21, 5)]
    iters = 100

    expected_results(lifts, iters, t_bayes, "../../plots/bayes_"+str(interval)+"_"+str(conf)+"_"+str(mde)+"_"+str(ctr)+".png")
    #expected_results(lifts, iters, t_nh, "../../plots/nht_ctr_"+str(interval)+"_"+str(alpha)+"_"+str(power)+"_"+str(ctr)+".png")



    