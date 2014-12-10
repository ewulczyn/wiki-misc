

    %matplotlib inline
    %load_ext autoreload
    %autoreload 2
    import pandas as pd
    pd.options.display.mpl_style = 'default'
    import numpy as np
    from abtest_util import SimStream, DonationProb, EmpiricalDonationProb
    from bayesian_abtest import CostABTest
    from nh_abtest import NHABTest, samples_per_branch_calculator
    from abstract_abtest import expected_results, expected_results_by_lift, expected_results_by_interval

    The autoreload extension is already loaded. To reload it, use:
      %reload_ext autoreload



    #Define Ground Truth Click Through Rates
    # DonationProb class
    p_B = DonationProb(0.20)
    p_A = DonationProb(0.20)
    
    # Estimate Control CTR From Historical Data
    n = 10000000
    hist_data_B = SimStream(p_B).get_next_records(n)
    p_hat = EmpiricalDonationProb(hist_data_B, p_B.values)
    ci =  p_hat.p_donate_ci()
    print "CI over control:",  ci
    interval = round(1.0/ci[0])*50
    print "Evalaution Interval", interval
    
    #interest in lift vlaues:
    lifts = [-0.20, -0.10, -0.05, -0.025, -0.015,0.0, 0.015, 0.025, 0.05, 0.10, 0.20]


    CI over control: (0.19960272954397854, 0.19984840000000004, 0.20009380088280151)
    Evalaution Interval 250.0



    # Set Up Cost  AB Test
    
    cost = 0.0002
    #max_run = float('inf')  #this one is so clean it doesnt need a max_run arg
    iters = 1000
    #41950
    
    expected_results_by_interval(CostABTest,[None, None, interval,41950, cost], iters, p_hat, lifts)


    -0.2
    -0.1
    -0.05
    -0.025
    -0.015
    0.0
    0.015
    0.025
    0.05
    0.1
    0.2



![png](blog_experiments_files/blog_experiments_2_1.png)



    # Set Up NH  AB Test
    mde = 0.05
    alpha = 0.05
    power = 0.95
    max_run = samples_per_branch_calculator(p_hat, mde, alpha, power)
    print max_run
    #41950
    iters = 500
    #expected_results_by_lift(NHABTest,[None, None, interval, max_run, alpha], iters, p_hat, lifts)
    
    expected_results_by_interval(NHABTest,[None, None, interval, 10000 , alpha], iters, p_hat, lifts)

    10399.5860427
    -0.2
    -0.1
    -0.05
    -0.025
    -0.015
    0.0
    0.015
    0.025
    0.05
    0.1
    0.2



![png](blog_experiments_files/blog_experiments_3_1.png)



    interval





    250.0




    
