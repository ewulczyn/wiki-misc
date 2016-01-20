import numpy as np

def epoch_and_batch_iter(X,y, batch_size, num_epochs):
    """
    Generates a batch iterator for a dataset.
    """
    
    for epoch in range(num_epochs):
       yield batch_iter(X, y, batch_size)


def batch_iter(X, y, batch_size):
    X = X.values
    y = y.values
    m = len(y)
    num_batches_per_epoch = int(float(m)/batch_size) + 1

    shuffle_indices = np.random.permutation(np.arange(m))
    shuffled_X = X[shuffle_indices]
    shuffled_y = y[shuffle_indices]
    for batch_num in range(num_batches_per_epoch):
        start_index = batch_num * batch_size
        end_index = min((batch_num + 1) * batch_size, m)
        yield shuffled_X[start_index:end_index], shuffled_y[start_index:end_index]
