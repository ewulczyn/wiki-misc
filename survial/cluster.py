from db_utils import query_s1
import pandas as pd
from sklearn.preprocessing import StandardScaler
from time import time
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns



def query_career_cluster_data():
    query = """
    SELECT
    AVG(cla_0) as avg_monthly_edits_0,
    STD(cla_0) as var_monthly_edits_0,
    AVG(cla_1) as avg_monthly_edits_1,
    STD(cla_1) as var_monthly_edits_1,
    AVG(cla_2) as avg_monthly_edits_2,
    STD(cla_2) as var_monthly_edits_2,
    AVG(cla_3) as avg_monthly_edits_3,
    STD(cla_3) as var_monthly_edits_3,
    AVG(cla_tot) as avg_monthly_edits_tot,
    STD(cla_tot) as var_monthly_edits_tot,
    AVG(friend_count) as avg_friend_count,
    STD(friend_count) as var_friend_count,
    COUNT(*) as num_active_months,
    SUM(cala_0)/SUM(cla_0) as percent_edits_got_archived_0,
    SUM(crla_0)/SUM(cla_0) as percent_edits_got_reverted_0
    FROM staging.leila_edits
    GROUP BY user_id
    """

    df =  query_s1(query, {}).fillna(0)
    return df
    



def query_monthly_behaviour_data():
    query = """
    SELECT
    CAST(cla_0 AS INT) AS main,
    CAST(cla_1 AS INT) AS talk,
    CAST(cla_2 AS INT) AS user,
    CAST(cla_3 AS INT) AS usertalk,
    CAST(cla_tot AS INT) AS total
    FROM staging.leila_edits
    WHERE cla_tot < 300
    LIMIT 100000
    """
    return query_s1(query, {}).fillna(0)



def project(X):
    pca = PCA(n_components=2).fit(X)
    print pca.explained_variance_ratio_ 
    print pca.components_
    reduced_data = pca.transform(X)
        #sns.kdeplot(reduced_data[:, 0], reduced_data[:, 1], shade=True)
    with sns.axes_style("white"):
        sns.jointplot(reduced_data[:, 0], reduced_data[:, 1], kind="kde");
    plt.show()
    plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)
    # Plot the centroids as a white X
    return reduced_data
