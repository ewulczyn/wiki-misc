from db_utils import query_s1
from sklearn.ensemble import RandomForestClassifier
from sklearn import cross_validation
from sklearn.cross_validation import StratifiedKFold, StratifiedShuffleSplit
from sklearn import svm, grid_search
import multiprocessing
from sklearn.metrics import roc_curve, auc, precision_recall_curve, f1_score, accuracy_score, accuracy_score, precision_score, recall_score, roc_auc_score, precision_recall_fscore_support
import logging
import numpy as np






def query_classification_data(**params):
    """
    parameters:
        horizon_month
        prediction_month
        n
    """

    query = """
    
    SELECT
    CAST(x.cla_0 AS INT) as num_edits_0,
    CAST(x.cla_1 AS INT) as num_edits_1,
    CAST(x.cla_2 AS INT) as num_edits_2,
    CAST(x.cla_3 AS INT) as num_edits_3,
    CAST(x.cla_tot AS INT) as num_edits_total,
    CAST(x.cla_tot - x.cla_0 - x.cla_1 - x.cla_2 - x.cla_3 AS INT) as num_edits_rest,
    CAST(x.v_no_months AS INT) as months_since_registration,
    CAST(x.aeb_no_months AS INT) as months_since_5_edits,
    CAST(x.talk_counts AS INT) as talk_counts,
    CAST(x.friend_count AS INT) as friend_count,
    z.num_edits_to_date_0,
    z.num_edits_to_date_1,
    z.num_edits_to_date_2,
    z.num_edits_to_date_3,
    z.num_edits_to_date_tot,
    z.num_edits_got_archived_to_date_0,
    z.num_edits_got_reverted_to_date_0,
    CASE
        WHEN y.user_id IS NOT NULL THEN 1
        ELSE 0
    END as survive
    FROM
    (SELECT * from staging.leila_edits
    WHERE month = %(horizon_month)s ) x
    LEFT JOIN
    (SELECT distinct(user_id) 
    FROM staging.leila_edits
    WHERE month <= %(prediction_month_end)s 
    AND month >= %(prediction_month_start)s  
    ) y
    ON (x.user_id = y.user_id)
    LEFT JOIN
    (SELECT user_id,
     SUM(cla_0) as num_edits_to_date_0,
     SUM(cla_1) as num_edits_to_date_1,
     SUM(cla_2) as num_edits_to_date_2,
     SUM(cla_3) as num_edits_to_date_3,
     SUM(cla_tot) as num_edits_to_date_tot,
     SUM(cala_0) as num_edits_got_archived_to_date_0,
     SUM(crla_0) as num_edits_got_reverted_to_date_0
    FROM staging.leila_edits
    WHERE month <= %(horizon_month)s 
    GROUP BY user_id) z
    ON (x.user_id = z.user_id)
    ORDER BY RAND()
    LIMIT %(n)s
    """

    return query_s1(query, params)


def get_scores(model, X):
    try:
        scores = model.decision_function(X)
    except:
        scores = model.predict_proba(X)[:, 1]
    return scores



def cvcv(X, y, folds, clf, param_grid, scoring, X_test = None, y_test = None):
    """
    Determine the best model by cross-validating:
        1. spliting data in test and training fold
        2. running cross_validation on training fold
        3. running cv model on test data
    """

    if X_test is not None:
        model = get_optimal_model (X, y, folds, clf, param_grid, scoring)

        y_pred = model.predict(X_test)
        scores = get_scores(model, X_test)

        print("Test ROC: %f" % roc_auc_score( y_test, scores))
        print("Test F1: %f" % f1_score( y_test, y_pred))

        print(classification_report(y_test, y_pred))



    else:
        cv = cross_validation.StratifiedKFold(y, n_folds = folds)
        cv_precision = []
        cv_recall = []
        cv_fscore = []
        cv_support = []
        roc = []

        for i, (train, test) in enumerate(cv):
            model = get_optimal_model (X[train], y[train], folds, clf, param_grid, scoring)
            logging.info("Finished Training fold %d" %(i))
            y_pred = model.predict(X[test])
            scores = get_scores(model, X[test])

            cv_report = precision_recall_fscore_support(y[test],y_pred)
            cv_precision.append(cv_report[0])
            cv_recall.append(cv_report[1])
            cv_fscore.append(cv_report[2])
            cv_support.append(cv_report[3])
            roc.append(roc_auc_score( y[test], scores))
            
            print "Fold Precision Class 0: %0.4f" %(cv_report[0][0])
            print "Fold Precision Class 1: %0.4f" %(cv_report[0][1])
            print "Fold Recall Class 0: %0.4f" %(cv_report[1][0])
            print "Fold Recall Class 1: %0.4f" %(cv_report[1][1])
            print "Fold F-Score Class 0: %0.4f" %(cv_report[2][0])
            print "Fold F-Score Class 1: %0.4f" %(cv_report[2][1])

        
        
        precision0 = map(lambda x:x[0],cv_precision)
        precision1 = map(lambda x:x[1],cv_precision)
        recall0 = map(lambda x:x[0],cv_recall)
        recall1= map(lambda x:x[1],cv_recall)
        fscore0 = map(lambda x:x[0],cv_fscore)
        fscore1 = map(lambda x:x[1],cv_fscore)
        support0 = map(lambda x:x[0],cv_support)
        support1 = map(lambda x:x[1],cv_support)

        print "\n\n\nAggregate Test Performace\n"

        print "ROC: %0.4f, (%0.4f)" %(np.mean(roc),np.std(roc))
        print "Precision: %0.4f, (%0.4f)" %(np.mean(cv_precision),np.std(cv_precision))
        print "Recall: %0.4f, (%0.4f)" %(np.mean(cv_recall),np.std(cv_recall))
        print "F-Score: %0.4f, (%0.4f)" %(np.mean(cv_fscore),np.std(cv_fscore))
        print "Support: %0.4f, (%0.4f)" %(np.mean(cv_support),np.std(cv_support))
        print "\n"
        print "Precision Class 0: %0.4f, (%0.4f)" %(np.mean(precision0),np.std(precision0))
        print "Precision Class 1: %0.4f, (%0.4f)" %(np.mean(precision1),np.std(precision1))
        print "Recall Class 0: %0.4f, (%0.4f)" %(np.mean(recall0),np.std(recall0))
        print "Recall Class 1: %0.4f, (%0.4f)" %(np.mean(recall1),np.std(recall1))
        print "F-Score Class 0: %0.4f, (%0.4f)" %(np.mean(fscore0),np.std(fscore0))
        print "F-Score Class 1: %0.4f, (%0.4f)" %(np.mean(fscore1),np.std(fscore1))
        print "Support Class 0: %0.4f, (%0.4f)" %(np.mean(support0),np.std(support0))
        print "Support Class 1: %0.4f, (%0.4f)" %(np.mean(support1),np.std(support1))




def save_model(X, y, feature_space, folds, clf, param_grid, scoring, outfile):
    model = get_optimal_model (X, y, folds, clf, param_grid, scoring)
    #clf = svm.LinearSVC(C = 0.01)
    #model = clf.fit(X,y)
    joblib.dump(model, outfile, compress=9)
    pickle.dump(feature_space, open(outfile+'.feature_space','wb'))




def cv (X, y, folds, clf, param_grid, scoring):
    """
    Determine the best model via cross validation. This should be run on training data.
    """

    print "\n\n\nDoing Gridsearch\n"

    strat_cv = cross_validation.StratifiedKFold(y, n_folds = folds)
    model = grid_search.GridSearchCV(cv  = strat_cv, estimator = clf, param_grid  = param_grid, scoring = scoring) #n_jobs=multiprocessing.cpu_count()
    model = model.fit(X,y)
    # model trained on all data
    y_pred = model.predict(X)
    scores = get_scores(model, X)
    print "Best Model Train ROC AUC: %f" % roc_auc_score(y, scores)
    print "Best Model Train F1: %f" % f1_score(y, y_pred)


    print("\nBest parameters set found:")
    best_parameters, score, _ = max(model.grid_scores_, key=lambda x: x[1])
    print(best_parameters, score)
    print "\n"
    print("Grid scores:")
    for params, mean_score, scores in model.grid_scores_:
        print("%0.3f (+/-%0.03f) for %r"
              % (mean_score, scores.std() / 2, params))

    return model


