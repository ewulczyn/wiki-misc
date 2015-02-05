from db_utils import query_s1
import pandas as pd
from sklearn.preprocessing import StandardScaler
from time import time
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA, KernelPCA
from sklearn.preprocessing import scale
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns



def query_career_cluster_data():

    """
    AVG(cla_2) as avg_monthly_edits_2,
    STD(cla_2) as var_monthly_edits_2,
    AVG(cla_3) as avg_monthly_edits_3,
    STD(cla_3) as var_monthly_edits_3,
    AVG(friend_count) as avg_friend_count,
    STD(friend_count) as var_friend_count,
    AVG(cla_tot) as avg_monthly_edits_tot,
    STD(cla_tot) as var_monthly_edits_tot,
    COUNT(*) as num_active_months,
    SUM(cala_0)/SUM(cla_0) as percent_edits_got_archived_0,
    SUM(crla_0)/SUM(cla_0) as percent_edits_got_reverted_0
    """



    query = """
    SELECT
    AVG(cla_0) as avg_monthly_edits_0,
    STD(cla_0) as var_monthly_edits_0,
    AVG(cla_1) as avg_monthly_edits_1,
    STD(cla_1) as var_monthly_edits_1
    FROM staging.leila_edits
    GROUP BY user_id
    HAVING COUNT(*) > 5
    LIMIT 50000
    """

    df =  query_s1(query, {}).fillna(0)
    return df
    



def query_monthly_behaviour_data():
    query = """
    SELECT
    cla_0/cla_tot AS percent_main,
    cla_1/cla_tot AS percent_talk,
    cla_2/cla_tot AS percent_user,
    cla_3/cla_tot AS percent_usertalk
    FROM staging.leila_edits
    WHERE month = '2013-05-31'
    """
    return query_s1(query, {}).fillna(0)



def project(X, kde = False, kernel = False, gamma = 10):
    if kernel:
        kpca = KernelPCA(kernel="rbf", fit_inverse_transform=True, gamma=gamma)
        reduced_data = kpca.fit_transform(X)
    else:
        pca = PCA(n_components=2).fit(X)
        print pca.explained_variance_ratio_ 
        print pca.components_
        reduced_data = pca.transform(X)
    if kde:
        with sns.axes_style("white"):
            sns.jointplot(reduced_data[:, 0], reduced_data[:, 1], kind="kde");
        plt.show()
    plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)
    return reduced_data
