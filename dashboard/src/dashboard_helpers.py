import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from data_retrieval import get_banner_data, HiveBannerDataRetriever, OldBannerDataRetriever
from datetime import timedelta
from stats_utils import *


"""
This module contains the Test class
"""


class Test(object):
    def __init__(self, *args, **kwargs):
        self.names = list(set(args))
        self.start = kwargs.get('start', None)
        self.stop = kwargs.get('stop', None)
        self.num_workers = kwargs.get('num_workers', 1)
        self.hive = kwargs.get('hive', False)

        if self.hive:
            self.data = get_banner_data(HiveBannerDataRetriever, self.names, self.start, self.stop)
        else:
            self.data = get_banner_data(OldBannerDataRetriever, self.names, self.start, self.stop)

        for name in self.names:
            self.data[name]['clean_donations'] = self.get_clean_donations(self.data[name]['donations'])


    def get_clean_donations(self, donations):
        clean_donations =  donations[np.abs(donations.amount-donations.amount.mean()) <= (3*donations.amount.std())]
        return clean_donations
    def combine(self, names, combination_name):

        """
        Allows for the logical combination of data from banners
        Data from all banners in names will the combined
        The combination can be accessed by the given combination_name
        """

        if combination_name in self.names:
            print "The combination_name is already in use"
            return

        if len(set(names).difference(self.names)) != 0:
            print "One of the banners is not known to the test object"
            return

        #reduce set to be unique
        names = list(set(names))

        # add new data dict
        self.data[combination_name] = {}
        #combine donations
        combined_donations = pd.concat([self.data[name]['donations'] for name in names], axis=0)
        combined_donations = combined_donations.sort()
        self.data[combination_name]['donations'] = combined_donations

        #combine clicks
        combined_clicks = pd.concat([self.data[name]['clicks'] for name in names], axis=0)
        combined_clicks = combined_clicks.sort()
        self.data[combination_name]['clicks'] = combined_clicks

        #combine impressions
        combined_impressions = pd.concat([self.data[name]['impressions'] for name in names], axis=0)
        combined_impressions = combined_impressions.groupby(combined_impressions.index).sum()
        self.data[combination_name]['impressions'] = combined_impressions

        self.data[combination_name]['clean_donations'] = self.get_clean_donations(self.data[combination_name]['donations'])

        self.names.append(combination_name)


    def ecom(self, *args):

        """
        One might beinterested in combining data from different banners into one
        new synthetic banner. Say you ran a test where people who saw banner B1 
        on their first impression,saw banner B3 on their next impressions.
        You might want to combine data from these two banners to do analysis on
        the aggregate data from B1 and B3.
        The combine function takes a list of banners used in the initialization
        of the test object t and combines them under the new name combination_name.
        """

        # set up list of banner to process
        d = {}
        if len(args) == 0:
            names = self.names
        else:
            names = args

        # Step through metrics and compute them for each banner

        # hive data can gives stats on how much traffic was allocated to a banner
        if self.hive: 
            d['traffic'] = [self.data[name]['traffic']['count'].sum() for name in names]

        d['impressions'] = [self.data[name]['impressions']['count'].sum() for name in names]
        d['clicks'] = [self.data[name]['clicks'].shape[0] for name in names]
        d['amount'] = [self.data[name]['donations']['amount'].sum() for name in names]
        d['donations'] = [self.data[name]['donations'].shape[0] for name in names]
        d['amount_ro'] = [self.data[name]['clean_donations']['amount'].sum() for name in names]
        d['max'] = [self.data[name]['donations']['amount'].max() for name in names]
        d['median'] = [self.data[name]['donations']['amount'].median() for name in names]
        d['avg'] = [self.data[name]['donations']['amount'].mean() for name in names]
        d['avg_ro'] = [self.data[name]['clean_donations']['amount'].mean() for name in names]

        d = pd.DataFrame(d)
        d.index = names

        # metrics computed from above metrics
        d['clicks/i'] = d['clicks'] / d['impressions']
        d['dons/i'] = d['donations'] / d['impressions']
        d['amount/i'] = d['amount'] / d['impressions']
        d['amount_ro/i'] = d['amount_ro'] / d['impressions']
        d['dons/clicks'] = d['donations'] / d['clicks']


        #Define the metrics in the order requested by Megan
        column_order = [
        'donations',
        'impressions',
        'dons/i',
        'amount',
        'amount/i',
        'clicks',
        'clicks/i',
        'dons/clicks',
        'amount_ro',
        'amount_ro/i',
        'max',
        'median',
        'avg',
        'avg_ro']

        # put hive traffic data into df if available
        if self.hive:
            column_order.insert(1, 'traffic')
        d = d[column_order]

        return d.sort()


    def get_payment_method_details(self, *args):

        """
        A banner usually gives several payment options for users.
        This function returns a dataframe showing how many people clicked on each payment method, 
        how many successful donations came from each payment method,
        the percent of donations that came from each method,
        the total raised for each method,
        the average raised for reach method, where outliers where removed
        """

        # set up list of banner to process
        if len(args) == 0:
            names = self.names
        else:
            names = args


        ds = []

        #Define the metrics in the order requested by Megan
        column_order = [
        'name',
        'donations',
        'clicks',
        'conversion_rate',
        'percent clicked on',
        'percent donated on',
        'total_amount',
        'ave_amount_ro'
        ]
        # Step through metrics and compute them for each banner

        for name in names:

            clicks = self.data[name]['clicks']['payment_method'].value_counts()
            donations = self.data[name]['donations']['payment_method'].value_counts()
            donations_sum = self.data[name]['donations'].groupby(['payment_method']).apply(lambda x: x.amount.sum())
            ave = self.data[name]['clean_donations'].groupby(['payment_method']).apply(lambda x: x.amount.mean())
            df = pd.concat([donations, clicks, ave, donations_sum], axis=1)
            df.columns = ['donations', 'clicks', 'ave_amount_ro', 'total_amount']

            # metrics computed from above metrics
            df['conversion_rate'] = 100* df['donations'] / df['clicks']
            df['percent clicked on'] = 100*df['clicks'] / df['clicks'].sum()
            df['percent donated on'] = 100*df['donations'] / df['donations'].sum()
            df['name'] = name

            #Put the metrics in the order requested by Megan

            df = df[column_order]
            ds.append(df)


        df = pd.concat(ds)
        df.index = pd.MultiIndex.from_tuples(zip(df['name'], df.index))
        del df['name']
        df = df.sort()

        return df


    def get_traffic_stats(self, *args):

        """
        The data in hive gives us all traffic allocated to banner.
        When a pageview is allocated to a banner it can the shown or not.
        If it is not shown, there can be multiple reasons.
        The hive data also tags spiders, so we can see if a spider was active during the test 
        """

        if not self.hive:
            print "Test object needs to be instantiated with keyword argument hive=True"
            return

        # set up list of banner to process
        if len(args) == 0:
            names = self.names
        else:
            names = args

        ds = []
        for name in names:
            df = self.data[name]['traffic'].groupby(['result', 'reason', 'spider']).sum()
            df = df.rename(columns={'count':name}).transpose()
            ds.append(df)

        df = pd.concat(ds)
        df = df.sort()
        return df



    def plot_donations_over_time(self, *args, **kwargs):
        # set up list of banner to process
        if len(args) == 0:
            names = self.names
        else:
            names = args

        #process keyword arguments
        window = kwargs.get('smooth', 10)
        start = kwargs.get('start', '2000')
        stop = kwargs.get('stop', '2050')
        amount = kwargs.get('amount', False)

        # helper function to join impression and donation data, very naive implementation
        def get_p_over_time(donations, impressions, window):
            donations['donation'] = 1
            d = donations.groupby(lambda x: (x.year, x.month, x.day, x.hour, x.minute)).sum()
            d.index = d.index.map(lambda t: pd.datetime(*t))
            d2 = impressions.join(d)
            d2 = d2.fillna(0)
            d2['d_window'] = 0
            d2['amount_window'] = 0
            m = d2.shape[0]

            for i in range(m):
                start = max(i-window, 0)
                end = min(i+window, m-1)
                d_window = d2.ix[start:end]
                p_window = d_window['donation'].sum()/d_window['count'].sum()
                d2['d_window'].ix[i] = p_window
                u_window = d_window['amount'].sum()/d_window['count'].sum()
                d2['amount_window'].ix[i] = u_window
            return d2

        # iterate over banners and generate plot
        fig = plt.figure(figsize=(10, 6), dpi=80)
        ax = fig.add_subplot(111)
        plt.xticks(rotation=70)
        formatter = DateFormatter('%Y-%m-%d %H:%M')

        plt.gcf().axes[0].xaxis.set_major_formatter(formatter)

        for name in names:
            d = get_p_over_time(self.data[name]['donations'], self.data[name]['impressions'], window)
            d = d[start:stop]
            if amount:
                ax.plot(d.index, d['amount_window'], label=name)
            else:
                ax.plot(d.index, d['d_window'], label=name)
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        if amount:
            plt.ylabel('amount per impression')
        else:
            plt.ylabel('donations per impression')
        plt.show()



    def plot_impressions(self, *args, **kwargs):

        """
        Plots impressions over the duration of the test
        Allow setting a time range
        And Smoothing by taking an avergae over a window of records
        """
        
        # set up list of banner to process
        if len(args) == 0:
            names = self.names
        else:
            names = args

        #process keyword arguments
        smooth = kwargs.get('smooth', 1)
        start = kwargs.get('start', '2000')
        stop = kwargs.get('stop', '2050')

        fig = plt.figure(figsize=(10, 6), dpi=80)
        ax = fig.add_subplot(111)
        plt.xticks(rotation=70)
        formatter = DateFormatter('%Y-%m-%d %H:%M')

        plt.gcf().axes[0].xaxis.set_major_formatter(formatter)

        for name in names:
            d = self.data[name]['impressions']
            d = d[start:stop]        
            d = pd.rolling_mean(d, smooth)
            ax.plot(d.index, d['count'], label=name)

        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        plt.ylabel('impressions')
        plt.show()


    def plot_utm_key(self, *args, **kwargs):

        """
        utm_key is strangely named. It is the number of impressions
        donors saw before donating
        """
        
        # set up list of banner to process
        if len(args) == 0:
            names = self.names
        else:
            names = args

        #process keyword arguments
        max_key = kwargs.get('max_key', 30)
        normalize = kwargs.get('normalize', True)

        fig = plt.figure(figsize=(10, 6), dpi=80)
        ax = fig.add_subplot(111)
        
        for name in names:
            d = pd.DataFrame(self.data[name]['donations']['impressions_seen'].value_counts())
            d = d.sort()
            if normalize:
                d[0] = d[0]/d[0].sum()
            d1 = d[:max_key]
            d1.loc[max_key+1] = d[max_key:].sum()

            ax.plot(d1.index, d1[0], marker='o', label=name)

        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        plt.xlabel('impressions seen before donating')
        ticks = [str(i) for i in range(0, max_key+1)]
        ticks.append(str(max_key+1)+"+")
        ax.set_xticks(range(0, max_key+2))

        ax.set_xlim([-0.5, max_key+1.5])
        ax.set_xticklabels(ticks)

        if normalize:
            plt.ylabel('fraction')
        else:
            plt.ylabel('counts')
        plt.show()    


    def compare_donation_amounts(self, a, b):

        """
        This one only operates in 2 banners.
        It gives very nice histogramms of donation amounts
        """
        a_cntr = Counter(np.floor(self.data[a]['donations']['amount']))
        b_cntr = Counter(np.floor(self.data[b]['donations']['amount']))

        keys = [int(s) for s in set(a_cntr.keys()).union(b_cntr.keys())]
        keys.sort()

        a_values = [a_cntr.get(k, 0) for k in keys]
        b_values = [b_cntr.get(k, 0) for k in keys]


        fig, ax = plt.subplots()
        fig.set_size_inches(15, 6)

        ind = 2.5*np.arange(len(keys))  # the x locations for the groups
        width = 1.2       # the width of the bars

        a_rects = ax.bar(ind, a_values, align='center', facecolor ='yellow', edgecolor='gray', label =a)
        b_rects = ax.bar(ind+width, b_values, align='center', facecolor ='blue', edgecolor='gray', label =b)

        ax.set_xticks(ind+width/2)
        ax.set_xticklabels(keys)
        ax.legend()


        def autolabel(rects):
            # attach some text labels
            for rect in rects:
                height = rect.get_height()
                ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                        ha='center', va='bottom')

        autolabel(a_rects)
        autolabel(b_rects)

        plt.show()




    def plot_show_hide(self, banner, stop ='2050', minutes =1):

        """
        For the given banner, gives the breakdown of 
        how many banners where shown and the reason for when the where hidden
        for every 'minutes' minutes
        """
        
        d = self.data[banner]['traffic'].copy()
        d = d[:stop]
        d = d.fillna('na')
        d = d[d.spider == False]
        d['dt'] = d.index
        d['dt'] = d['dt'].apply(lambda tm: tm - timedelta(minutes=(60 * tm.hour + tm.minute) % minutes))
        d.index = pd.MultiIndex.from_tuples(zip(d['dt'], d['result'], d['reason']))
        d = d[['count',]]
        
        d = d.groupby(d.index).sum()
        d.index = pd.MultiIndex.from_tuples(d.index)

        d = d.unstack(level=[1, 2])
        d.columns = d.columns.droplevel(0)
        
        fig = plt.figure(figsize=(24, 12), dpi=80)
        ax = fig.add_subplot(111)
        d.plot(ax =ax, kind='bar', stacked=True, legend=True)
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))





########### STATS FUNCTIONS #########


    def amount_stats(self, a, b, conf=95, rate='donations/impressions', remove_outliers=True):

        """
        Gives a confidence for difference in the dollars per 1000 impressions between banners a, b 

        a: string name of the A banner
        b: string name of the B banner
        conf: confidence level in [0, 100] for the confidence intervals.
        rate: there are two kinds of rates this function can handle:
            'donations/impressions': 
            'donations/clicks': donations per click
        remove_outliers: remove donations exceeding 3 standard deviations from the mean
        """

        t = rate.split('/')

        # use donation data with outliers removed by defualt
        if remove_outliers:

            a_event_values = self.data[a]['clean_donations']['amount']
            b_event_values = self.data[b]['clean_donations']['amount']

        else:
            a_event_values = self.data[a]['donations']['amount']
            b_event_values = self.data[b]['donations']['amount']

        trial_type = t[1]

        


        if trial_type == 'clicks':
            a_num_trials = self.data[a]['clicks'].shape[0]
            b_num_trials = self.data[b]['clicks'].shape[0]
            amount_ci = difference_in_means_confidence_interval(a_event_values, a_num_trials, b_event_values, b_num_trials, alpha=(100 - conf)/200.0)
            print "%s gives between $%0.4f and $%0.4f more $/clicks than %s" %(a, amount_ci[0], amount_ci[1], b)

        elif trial_type == 'impressions':
            a_num_trials = self.data[a]['impressions'].sum()
            b_num_trials = self.data[b]['impressions'].sum()
            amount_ci = difference_in_means_confidence_interval(a_event_values, a_num_trials, b_event_values, b_num_trials, alpha=(100 - conf)/200.0)
            print "%s gives between $%0.4f and $%0.4f more $/1000 impressions than %s" %(a, 1000*amount_ci[0], 1000*amount_ci[1], b)

        else:
            print "incorrect test argument"
            return

    
    def rate_stats(self, *args, ** kwargs):


        """
        usage: t.rate_stats(B1, B2, ...BN, conf = 95, rate = 'donations/impressions', plot = True)

        Args:
            Bi: string name of the ith banner
            conf: confidence level in [0, 100] for the confidence intervals.
            rate: there are three kinds of rates this function can handle:
                ''donations/impressions'': donations per impression
                'clicks/impressions': clicks per impression
                'donations/clicks': donations per click
        plot: whether to plot the distributions over the CTRs


        This function computes:
        P(Bi is Best): probability that banner Bi gives more donations per impression than all other banners

        Winers Lift: a 'conf' percent confidence interval on the percent lift in rate the winning banenr  has over the others
        CI: a 'conf' percent confidence interval for the rate of Bi
        

        """
        conf = kwargs.get('conf', 95)
        rate = kwargs.get('rate', 'donations/impressions')
        plot = kwargs.get('plot', True)

        if len(args) == 0:
            names = self.names
        else:
            names = args

        if rate == 'donations/impressions':
            d = pd.DataFrame.from_dict({name:get_beta_dist(self.data[name]['donations'].shape[0], self.data[name]['impressions'].sum()) for name in names})
            return print_rate_stats(d, conf, plot)

        elif rate == 'clicks/impressions':
            d = pd.DataFrame.from_dict({name:get_beta_dist(self.data[name]['clicks'].shape[0], self.data[name]['impressions'].sum()) for name in names})
            return print_rate_stats(d, conf, plot)

        elif rate == 'donations/clicks':
            d = pd.DataFrame.from_dict({name:get_beta_dist(self.data[name]['donations'].shape[0], self.data[name]['clicks'].shape[0]) for name in names})
            return print_rate_stats(d, conf, plot)
        

def print_rate_stats(dists, conf, plot):

    """
    Helper function to create a pandas datframe with rate statistics
    """

    if plot:
        plot_rate_dist(dists)
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
    


def plot_rate_dist(dists):
    """
    Helper function to plot the probability distribution over
    the donation rates (bayesian formalism)
    """
    fig, ax = plt.subplots(1, 1, figsize=(13, 3))

    bins = 50
    for name in dists.columns:
        ax.hist(dists[name], bins=bins, alpha=0.6, label=name)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    



def custom_amount_stats(a_event_values, a_num_trials, b_event_values, b_num_trials, conf =95):
    amount_ci = difference_in_means_confidence_interval(a_event_values, a_num_trials, b_event_values, b_num_trials, alpha = (100 - conf)/200.0)
    print "A gives between $%0.4f and $%0.4f more $/clicks than B" %(amount_ci[0], amount_ci[1])


def custom_rate_stats(a_num_events, a_num_trials, b_num_events, b_num_trials, conf=95, plot =True):
    a_dist = get_beta_dist(a_num_events, a_num_trials)
    b_dist = get_beta_dist(b_num_events, b_num_trials)
    d = pd.DataFrame.from_dict({'A':a_dist, 'B':b_dist})
    return print_rate_stats(d, conf, plot)

