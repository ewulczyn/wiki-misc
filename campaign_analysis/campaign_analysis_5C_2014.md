

    %matplotlib inline
    %load_ext autoreload
    %autoreload 2
    import pandas as pd
    import matplotlib.pyplot as plt
    pd.options.display.mpl_style = 'default'
    import collections
    
    from campaign_analysis import *


    The autoreload extension is already loaded. To reload it, use:
      %reload_ext autoreload



    start = '2014-11-28 00:00'
    stop = '2015-01-03 00:00'
    
    click = get_clicks(start, stop)
    don = get_donations(start, stop)
    imp = get_impressions(start, stop)

    
        SELECT
        DATE_FORMAT(CAST(ts as datetime), '%Y-%m-%d %H') as timestamp,  CONCAT_WS(' ', banner, utm_campaign) as name,
        COUNT(*) as n,
        ct.country as country
        FROM drupal.contribution_tracking ct, drupal.contribution_source cs
        WHERE  ct.id = cs.contribution_tracking_id
        AND ts BETWEEN 20141128000000 AND 20150103000000
        AND utm_medium = 'sitenotice'
        AND utm_campaign REGEXP .*
        GROUP BY DATE_FORMAT(CAST(ts as datetime), '%Y-%m-%d %H'),  CONCAT_WS(' ', banner, utm_campaign)
        
      country   n                                             name      timestamp
    0      US   1       B14_1027_enUS_ipd_hl_mr C14_enUS_ipd_lw_FR  2014-11-28 00
    1      FR   4          B14_1030_enFR_mob_gov_n C14_enFR_mob_FR  2014-11-28 00
    2      FR   2          B14_1030_enFR_mob_gov_y C14_enFR_mob_FR  2014-11-28 00
    3      CA  15  B14_1110_mlWW_dsk_lw_sym_lit C14_mlWW_dsk_lw_FR  2014-11-28 00
    4      NO  15  B14_1110_mlWW_dsk_lw_sym_std C14_mlWW_dsk_lw_FR  2014-11-28 00
    
        SELECT
        DATE_FORMAT(CAST(ts as datetime), '%Y-%m-%d %H') as timestamp,  CONCAT_WS(' ', banner, utm_campaign) as name,
        COUNT(*) as n,
        SUM(co.total_amount) as amount,
        ct.country as country
        FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
        WHERE  ct.id = cs.contribution_tracking_id
        AND co.id = ct.contribution_id
        AND ts BETWEEN 20141128000000 AND 20150103000000
        AND utm_medium = 'sitenotice'
        AND utm_campaign REGEXP .*
        GROUP BY DATE_FORMAT(CAST(ts as datetime), '%Y-%m-%d %H'),  CONCAT_WS(' ', banner, utm_campaign)
        
       amount country   n                                             name  \
    0   12.49      FR   1          B14_1030_enFR_mob_gov_y C14_enFR_mob_FR   
    1   18.71      JP   2  B14_1110_mlWW_dsk_lw_sym_lit C14_mlWW_dsk_lw_FR   
    2   63.75      NO   5  B14_1110_mlWW_dsk_lw_sym_std C14_mlWW_dsk_lw_FR   
    3    6.25      FR   1  B14_1117_enFR_dsk_top_txt_cof_p C14_enFR_dsk_FR   
    4  579.80      AU  46       B14_1120_enWW_dsk_tx_ev C14_enWW_dsk_lw_FR   
    
           timestamp  
    0  2014-11-28 00  
    1  2014-11-28 00  
    2  2014-11-28 00  
    3  2014-11-28 00  
    4  2014-11-28 00  



    


    desk_regs = {
        'Large Desk': '.*5C_lg.* C14_en5C_dec_dsk_FR',
        'Top Desk': '.*5C_tp.* C14_en5C_dec_dsk_FR',
    }


    mob_regs = {
        'Large Mobile': '.*_lg.* C14_en5C_dec_mob_FR',
        'Top Mobile': '.*_tp.* C14_en5C_dec_mob_FR'
    }
    
    ipd_regs = {
        'Large Ipad': '.*_lg.* C14_en5C_dec_ipd_FR',
        'Top Ipad': '.*_tp.* C14_en5C_dec_ipd_FR'
    }


Large:
    - first impression
    - appears only once per client
Top:
    - appears on every pageview
    - hidden for a week if the banner is closed
    - on 17th limited to five impressions since campaign start
    - on 24th limited to five impressions in the year
    - lifted limits for New Years

When you donate, impressions stop.

Staggered launch: Email, Desktop, Mobile + Ipad

##Impressions: top vs full


    config = {
                'hours' :1, 
                'start' : '2014-11-28 00:00',
                'ylabel': 'impressions per hour'
    }
    
    plot_by_time(imp, desk_regs, **config )



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_7_0.png)



    config = {
                'hours' :24, 
                'start': '2014-12-1 00',
                'ylabel': 'number of first time visitors since DEC 1'
    }
    
    plot_by_time(imp, {'Large Desk': '.*5C_lg.* C14_en5C_dec_dsk_FR'}, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_8_0.png)


By the end the campaign, we are still getting roughly 5 million fullscreen
pageviews per day! This is a proxy for the number of clients visiting for the
first time since Dec 2.


    config = {
                'hours' :1, 
                'start': '2014-12-1 00',
                'ylabel': 'impressions per hour'
    }
    plot_by_time(imp, mob_regs, **config )



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_10_0.png)


Difference in impression rate between banner types is much lower than on
desktop. Due to shorter sessions?


    config = {
                'hours' :1, 
                'ylabel': 'impressions per hour'
    }
    plot_by_time(imp, ipd_regs, **config )



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_12_0.png)



    

##Donations: top vs full


    config = {
                'hours' :1, 
                'ylabel': 'donations per hour'
    }
    
    plot_by_time(don, desk_regs, **config)



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_15_0.png)


Through out the campaign, the Large banner generates at least as many donations
as the top screen banner, except on the New Year's day push.


    config = {
                'hours' :1, 
                'start': '2014-12-8 00',
                'ylabel': 'donations per hour'
    }
    plot_by_time(don, mob_regs, **config )



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_17_0.png)


Same as above


    config = {
                'hours' :1, 
                'start': '2014-12-8 00',
                'ylabel': 'donations per hour'
    }
    plot_by_time(don, ipd_regs, **config )
    



![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_19_0.png)


On the Ipad, the top screen banners quickly lead to fewer donations than the
large screen  banners.

#Donation Rate: top vs full


    config = {
                'hours' :4, 
                'stop': '2015-01-01 00',
    
    
            }
    
    plot_rate_by_time(don, imp, desk_regs, **config)


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_22_0.png)


At launch, the large banner gets a donation from every 100 impressions!

At launch, the top screen banner is an order of magnitude less effective, (note:
this population did not donate on their full-screen  banner).

Top screen donation rate lifts around the 17th due to the limit on impressions
per client.

We have a remarkably high donation rate from infrequent visitors (see late Large
banner donation rate)




    config = {
                'hours' :24, 
                'start': '2014-12-8 00',
            }
    
    plot_rate_by_time(don, imp, mob_regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_24_0.png)



    config = {
                'hours' :24, 
                'start': '2014-12-8 00',
                'stop': '2014-12-30',
                'ylabel': 'click rate'
            }
    
    plot_rate_by_time(click, imp, mob_regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_25_0.png)


The donation rate of the large banner declines faster than on desktop.

Eventually it is no more effective than the top screen banner.

Donation rates are 5x lower. Note: email and desktop campaigns have already been
running for a week!!


    config = {
                'hours' :4, 
                'stop': '2015-01-01 10',
            }
    
    plot_rate_by_time(don, imp, ipd_regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_27_0.png)



    config = {
                'hours' :24, 
                'stop': '2015-01-01 10',
            }
    
    regs = collections.OrderedDict()
    
    regs['Large Desk']= '.*5C_lg.* C14_en5C_dec_dsk_FR'
    regs['Large Ipad'] = '.*_lg.* C14_en5C_dec_ipd_FR'
    regs['Large Mobile'] = '.*_lg.* C14_en5C_dec_mob_FR'
    
    
    
    plot_rate_by_time(don, imp, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_28_0.png)



    config = {
                'hours' :24, 
                'stop': '2015-01-01 10',
            }
    
    regs = collections.OrderedDict()
    regs['Top Desk']= '.*5C_tp.* C14_en5C_dec_dsk_FR'
    regs['Top Ipad'] = '.*_tp.* C14_en5C_dec_ipd_FR'
    regs['Top Mobile'] = '.*_tp.* C14_en5C_dec_mob_FR'
    
    plot_rate_by_time(don, imp, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_29_0.png)


The full screen banner eventually becomes less effective than the top screen
banner.


    plot_avg(don, desk_regs, hours = 24, stop = '2014-12-31')


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_31_0.png)


##AverEG Amount per Hour (Skip)


    config = {
            'hours' : 1, 
            'amount' : True,
            #'start' : '2014-12-2 19',
            'ylabel' : 'dollars per hour',
    }
    fig = plot_by_time(don, {'Desk':'.* C14_en5C_dec_dsk_FR'}, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_33_0.png)



    config = {
            'hours' : 1, 
            'amount' : True,
            'start' : '2014-12-8 00',
            'ylabel' : 'dollars per hour'
    }
    
    regs = {
        'Mobile':'.* C14_en5C_dec_mob_FR',
    }
    fig = plot_by_time(don, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_34_0.png)



    config = {
            'hours' : 1, 
            'amount' : True,
            'start' : '2014-12-8 00',
            'ylabel' : 'dollars per hour'
    }
    
    regs = {
        'Mobile':'.* C14_en5C_dec_ipd_FR',
    }
    fig = plot_by_time(don, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_35_0.png)


#Amount per Day


    config = {
            'hours' : 24, 
            'amount' : True,
            'start' : '2014-12-2 00',
            'ylabel' : 'dollars per day'
    }
    
    regs = {
        #'Desk + Mobile + Ipad':'.* C14_en5C_dec_*',
        ' TOP Desk + Mobile + Ipad':'.*_tp.* C14_en5C_dec_*',
        ' LARGE Desk + Mobile + Ipad':'.*_lg.* C14_en5C_dec_*'
    
    }
    fig = plot_by_time(don, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_37_0.png)


We make as much money on our first impression (with large banner) as we do on
all other impressions


    config = {
            'hours' : 24, 
            'amount' : True,
            'start' : '2014-12-3 00',
            #'stop'  : '2014-12-18 23:59',
            'ylabel' : 'dollars per day'
    }
    
    regs = collections.OrderedDict()
    regs['All']= '.* C14_en5C_dec*'
    regs['Desk'] = '.* C14_en5C_dec_dsk*'
    regs['Mobile'] = '.* C14_en5C_dec_mob*'
    regs['Ipad'] = '.* C14_en5C_dec_ipd*'
    
    
    
    fig = plot_by_time(don, regs, **config )


![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_39_0.png)


How do the different device campaigns interact? What is the marginal value of
the mobile cmapaigns?

With the data we have we cannot tell. The data supports all theories :).

## Cumulative Total per Day


    config = {
        'hours': 12,
        'amount': True,
        'cum' : True,
        'ylabel' : 'cumulative dollars',
        'interactive': False,
        'normalize' : True
    }
    
    regs = {
        'all-banner': '.*',
        'en5C_dec':'.* C14_en5C_dec*',
        'Desk en5C_dec':'.* C14_en5C_dec_dsk*',
        'Mobile en5C_dec':'.* C14_en5C_dec_mob*',
        'Ipad en5C_dec':'.* C14_en5C_dec_ipd*'
    
    }
    
    plot_by_time(don,regs , **config)
    
    # Get Total by 
    d_totals = pd.DataFrame()
    for name, reg in regs.items():
        counts = don.ix[don.name.str.match(reg).apply(bool)]['amount']
        d_totals[name] = [counts.sum()]
    d_totals




<div style="max-height:1000px;max-width:1500px;overflow:auto;">
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Desk en5C_dec</th>
      <th>all-banner</th>
      <th>en5C_dec</th>
      <th>Ipad en5C_dec</th>
      <th>Mobile en5C_dec</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td> 19185091.79</td>
      <td> 25809463.28</td>
      <td> 24283227.98</td>
      <td> 1948175.88</td>
      <td> 3149960.31</td>
    </tr>
  </tbody>
</table>
</div>




![png](campaign_analysis_5C_2014_files/campaign_analysis_5C_2014_42_1.png)


We have made half the money we will make a third of the way in


    # donations 
    
    regs = desk_regs
    d_totals = pd.DataFrame()
    for name, reg in regs.items():
        counts = don.ix[don.name.str.match(reg).apply(bool)]['amount']
        d_totals[name] = [counts.sum()]
    d_totals




<div style="max-height:1000px;max-width:1500px;overflow:auto;">
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Large Desk</th>
      <th>Top Desk</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td> 10944414.75</td>
      <td> 8239448.1</td>
    </tr>
  </tbody>
</table>
</div>




    # donations 
    
    regs = desk_regs
    d_totals = pd.DataFrame()
    for name, reg in regs.items():
        counts = don.ix[don.name.str.match(reg).apply(bool)]['n']
        d_totals[name] = [counts.sum()]
    d_totals





<div style="max-height:1000px;max-width:1500px;overflow:auto;">
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Large Desk</th>
      <th>Top Desk</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td> 944141</td>
      <td> 654684</td>
    </tr>
  </tbody>
</table>
</div>




    # impressions
    d_totals = pd.DataFrame()
    for name, reg in desk_regs.items():
        counts = imp.ix[imp.name.str.match(reg).apply(bool)]['n']
        d_totals[name] = [counts.sum()]
    d_totals




<div style="max-height:1000px;max-width:1500px;overflow:auto;">
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Large Desk</th>
      <th>Top Desk</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td> 228418400</td>
      <td> 759396000</td>
    </tr>
  </tbody>
</table>
</div>




    d_totals = pd.DataFrame()
    for name, reg in mob_regs.items():
        counts = imp.ix[imp.name.str.match(reg).apply(bool)]['n']
        d_totals[name] = [counts.sum()]
    d_totals




<div style="max-height:1000px;max-width:1500px;overflow:auto;">
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Large Mobile</th>
      <th>Top Mobile</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td> 191529500</td>
      <td> 338426700</td>
    </tr>
  </tbody>
</table>
</div>




    
