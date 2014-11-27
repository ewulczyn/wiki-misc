

    import numpy as np
    import matplotlib.pyplot as plt
    %matplotlib inline
    import pandas as pd
    



    def run(mu, sigma, p, n, r):
        rs = [r, ]
        
        for i in range(n):
            # create new idea
            lift = np.random.normal(mu, sigma)
            r_new = min(rs[-1]*(1.0+lift/100.0), 0.01)
            r_correct = max(r_new, rs[-1])
            r_incorrect = min(r_new, rs[-1])
    
            if np.random.binomial(1, p):
                rs.append(r_correct)
            else:
                rs.append(r_incorrect)
            
        return rs
            


    def compare_trajectories(mu, sigma, p1, p2, n, r, repeat = 10):
        fig = plt.figure(figsize=(10, 6), dpi=80)
        ax = fig.add_subplot(111)
        
        d = pd.DataFrame()
        for i in range(repeat):
            d[i] = run(mu, sigma, p1, n, r)
            
        d2 = pd.DataFrame()
        d2['mean'] = d.mean(axis=1)
        d2['lower'] = d2['mean'] + 2*d.std(axis=1)
        d2['upper'] = d2['mean'] - 2*d.std(axis=1)
    
        ax.plot(d2.index, d2['mean'], label= 'p1=%0.3f'%p1)
        ax.fill_between(d2.index, d2['lower'], d2['upper'], alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)
        
        
        d = pd.DataFrame()
        for i in range(repeat):
            d[i] = run(mu, sigma, p2, n, r)
            
        d2 = pd.DataFrame()
        d2['mean'] = d.mean(axis=1)
        d2['lower'] = d2['mean'] + d.std(axis=1)
        d2['upper'] = d2['mean'] - d.std(axis=1)
    
        ax.plot(d2.index, d2['mean'], label= 'p2=%0.3f'%p2)
        ax.fill_between(d2.index, d2['lower'], d2['upper'], alpha=0.31, edgecolor='#3F7F4C', facecolor='0.75',linewidth=0)
        ax.set_xlabel('num tests')
        ax.set_ylabel('donation rate')
        
        ax.plot(d2.index, [r]*(n+1), label = 'base rate')
        ax.legend()
            
        


    def plot_improvements(mu, sigma):
        x = np.arange(-45.0, 45.0, 0.5)
        plt.xticks(np.arange(-45.0, 45.0, 5))
        plt.plot(x, 1/(sigma * np.sqrt(2 * np.pi)) *np.exp( - (x - mu)**2 / (2 * sigma**2) ))



    #Distribution over % Improvements
    mu = -20.0
    sigma = 12.0
    
    plot_improvements(mu, sigma)
    
    #Proability of making the correct decision
    p1 = 0.6
    p2 = 0.99
    
    #number of trials
    n = 1000
    #base donation rate
    r = 0.0007


![png](simulation_files/simulation_4_0.png)



    compare_trajectories(mu, sigma, p1, p2, n, r, repeat = 100)


![png](simulation_files/simulation_5_0.png)



    0.007*(1.1**4)




    0.010248700000000003




    
