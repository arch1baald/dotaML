import os
import pickle

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics


LANES = {
    'safe': 1,
    'mid': 2,
    'off': 3,
    'jungle': 4
}


def data_preparation(df, id_columns):
    categorical_columns = [
        c for c in df.columns
        if df[c].dtype.name == 'object' and c not in id_columns
    ]
    numerical_columns = [c for c in df.columns if df[c].dtype.name != 'object' and c not in id_columns]
    valid_columns = numerical_columns + categorical_columns

    X = df[valid_columns]
    y = df['win']

    df_dummies = pd.get_dummies(X[categorical_columns])
    X = pd.concat([X, df_dummies], axis=1)
    X.drop(categorical_columns, axis=1, inplace=True)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=12345)
    N_train, _ = X_train.shape
    N_test, _ = X_test.shape
    print(N_train, N_test)
    return X_train, X_test, y_train, y_test


def train_models():
    df_matches = pd.read_csv('./data/dataset.csv', index_col=0)
    id_columns = [
        'hero_id', 'match_id', 'account_id', 'team_id',
        'start_time', 'id', 'datetime', 'leaguename',
        'win'
    ]

    model = {}
    for role, df_role in df_matches.groupby('lane_role'):
        print('Model for role: {}'.format(role))
        X_train, X_test, y_train, y_test = data_preparation(df_role, id_columns)

        lm = LogisticRegression(penalty='l1', C=1, fit_intercept=False)
        lm.fit(X_train, y_train)
        model[role] = lm

        print("Train accuracy = %s" % metrics.accuracy_score(y_train, lm.predict(X_train)))
        print("Test accuracy = %s" % metrics.accuracy_score(y_test, lm.predict(X_test)))
        print("Train AUC = %s" % metrics.roc_auc_score(y_train, lm.predict_proba(X_train)[:, 1]))
        print("Test AUC = %s" % metrics.roc_auc_score(y_test, lm.predict_proba(X_test)[:, 1]))
        print("Train Recall = %s" % metrics.recall_score(y_train, lm.predict(X_train)))
        print("Test Recall = %s" % metrics.recall_score(y_test, lm.predict(X_test)))
        print("Train Precision = %s" % metrics.precision_score(y_train, lm.predict(X_train)))
        print("Test Precision = %s" % metrics.precision_score(y_test, lm.predict(X_test)))
        print()

    for role, model_obj in model.items():
        file_name = 'model_{}.sklearn'.format(role)
        file_path = os.path.join(os.path.abspath('data'), file_name)
        with open(file_path, 'wb') as model_file:
            pickle.dump(model_obj, model_file)


if __name__ == '__main__':
    train_models()
