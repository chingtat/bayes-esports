import numpy as np
import sklearn
import pickle


class Predictor():
    def __init__(self, model_path='code_challenge_model.p'):
        self.model = pickle.load(open(model_path, 'rb'))
    
    def predict(self, X):
        assert len(np.shape(X)) in [1, 2], "X should be 1-dim or 2-dim array"
        if len(np.shape(X)) == 1:
            X = np.array([X, ])

        return self.model.predict_proba(X)[:, 1]


if __name__ == '__main__':
    import pandas as pd

    # test predictor
    df = pd.read_csv('code_challenge_data1.csv', index_col=0)
    model = Predictor()
    print(model.predict(df.iloc[0].values))