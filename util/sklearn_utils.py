from sklearn import cross_validation, grid_search
import copy
from sklearn.cross_validation import train_test_split
import sklearn
from scipy.stats import spearmanr
from sklearn.metrics import f1_score, roc_auc_score,accuracy_score, make_scorer
from sklearn.metrics import r2_score, mean_squared_error
from pprint import pprint

def rmse(x, y):
    return mean_squared_error(x,y)**0.5

def f1(x, y):
    sr = f1_score(x, y)
    print ('F1: %2.5f' % sr)
    return sr

def auc(y_true,y_pred):
    roc = roc_auc_score(y_true, y_pred)
    print ('ROC: %2.5f \n' % roc)
    return roc

def acc(y_true,y_pred):    
     r2 = accuracy_score(y_true, y_pred)
     print ('Accuracy: %2.5f' % r2)
     return r2

def multi_score_classification(y_true,y_pred):   
    f1(y_true,y_pred) #set score here and not below if using MSE in GridCV
    acc(y_true,y_pred)
    score = auc(y_true,y_pred)
    return score

def multi_scorer_classification():
    return make_scorer(multi_score_classification, greater_is_better=True) # change for false if using MSE


def spearman(x, y):
    sr = spearmanr(x, y)[0]
    print ('Spearman: %2.5f \n' % sr)
    return sr

def RMSE(y_true,y_pred):
    rmse_val = rmse(y_true, y_pred)
    print ('RMSE: %2.5f' % rmse_val)
    return rmse_val

def R2(y_true,y_pred):    
     r2 = r2_score(y_true, y_pred)
     print ('R2: %2.5f' % r2)
     return r2

def multi_score_regression(y_true,y_pred):    
    RMSE(y_true,y_pred) #set score here and not below if using MSE in GridCV
    R2(y_true,y_pred)
    score = spearman(y_true,y_pred)
    return score

def multi_scorer_regression():
    return make_scorer(multi_score_regression, greater_is_better=True) # change for false if using MSE

  
def cv (X, y, folds, alg, param_grid, regression):
    """
    Determine the best model via cross validation. This should be run on training data.
    """
    if regression:
        scoring = multi_scorer_regression()
    else:
        scoring = multi_scorer_classification()
        
    print ("\n\n\nDoing Gridsearch\n")

    kfold_cv = cross_validation.KFold(X.shape[0], n_folds=folds, shuffle=True)
    model = grid_search.GridSearchCV(cv  = kfold_cv, estimator = alg, param_grid = param_grid, scoring = scoring, n_jobs=1)
    model = model.fit(X,y)
    # model trained on all data
    y_pred = model.predict(X)
    
    if regression:
        print ("Best Model Train RMSE: %f" % rmse(y, y_pred))
        print ("Best Model Train Spearman %f" % spearman(y, y_pred))
    else:
        print ("Best Model Train AUC: %f" % roc_auc_score(y, y_pred))
        print ("Best Model Train F1 %f" % f1_score(y, y_pred))
        print ("Best Model Train Accuracy %f" % accuracy_score(y, y_pred))
        


    print("\nBest parameters set found:")
    best_parameters, score, _ = max(model.grid_scores_, key=lambda x: x[1])
    print(best_parameters, score)
    print ("\n")
    print("Grid scores:")
    for params, mean_score, scores in model.grid_scores_:
        print("%0.5f (+/-%0.05f) for %r"
              % (mean_score, scores.std() / 2, params))

    return model





def get_scores(model, X):
    try:
        scores = model.decision_function(X)
    except:
        scores = model.predict_proba(X)[:, 1]
    return scores



def evaluate(X, y, alg, regression, retrain_model = False, verbose = False, X_test = None, y_test = None ):
    
    
    if X_test is not None and y_test is not None:
        X_train = X
        y_train = y
    else:
        X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.33, random_state=42)

    model = alg.fit(X_train,y_train)
    y_train_pred = model.predict(X_train)
    y_test_pred  = model.predict(X_test)
    
    if not regression:
        y_train_proba = get_scores(model,X_train)
        y_test_proba = get_scores(model,X_test)
        
        
    
    if regression:

        metrics = {'RMSE': {
                                'train': -rmse(y_train, y_train_pred),
                                'test': -rmse(y_test, y_test_pred)
                            }, 
                    'Spearman': {
                                    'train': spearmanr(y_train, y_train_pred)[0],
                                    'test': spearmanr(y_test, y_test_pred)[0]
                                }
                    }
        
    else :

        metrics = {'AUC': {
                                'train': roc_auc_score(y_train, y_train_proba),
                                'test': roc_auc_score(y_test, y_test_proba)
                            }, 
                    'F1': {
                                    'train': f1_score(y_train, y_train_pred),
                                    'test': f1_score(y_test, y_test_pred)
                                },

                    'Accuracy': {
                                    'train': accuracy_score(y_train, y_train_pred),
                                    'test': accuracy_score(y_test, y_test_pred)
                                }
                    }
        
        
    if verbose:
        pprint (metrics)
    
    if retrain_model:
        model =  alg.fit(X,y)
        
         
    return model, metrics[metric]['train'], metrics[metric]['test'], metrics


# Needs Work in order to be generalized
def seq_forw_select(features, max_k, importance_model, print_steps=True):
    features = copy.deepcopy(features)
    # Initialization
    feat_sub = []
    feat_sub_set_names = []
    k = 0
    d = len(features)
    if max_k > d:
        max_k = d
  
    while True:
        print ('\n\n')
        available_features = list(features.keys())
        # Inclusion step
        if print_steps:
            print('Available Features', available_features)
        _, train_metric, crit_func_max, metrics_max = importance_model.build(feat_sub + features[available_features[0]])
        if print_steps:
                print (feat_sub_set_names + [available_features[0]], ': ', (train_metric, crit_func_max))
                
        best_feat = available_features[0]
        for x in available_features[1:]:
            _, train_metric, crit_func_eval, metrics = importance_model.build(feat_sub + features[x])
            if print_steps:
                print (feat_sub_set_names + [x], ': ', (train_metric, crit_func_eval))
            if crit_func_eval > crit_func_max:
                crit_func_max = crit_func_eval
                best_feat = x
                metrics_max = metrics
        feat_sub += features[best_feat]
        feat_sub_set_names.append(best_feat)
        del features[best_feat]
        
        if print_steps:
            print('Winning Subset: ', feat_sub_set_names, ': ', metrics_max)
        

        # Termination condition
        k = len(feat_sub_set_names)
        if k == max_k:
            break