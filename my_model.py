import utils
import etl
import models_partc
import cross
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
import os
import pandas as pd

#Note: You can reuse code that you wrote in etl.py and models.py and cross.py over here. It might help.
# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

'''
You may generate your own features over here.
Note that for the test data, all events are already filtered such that they fall in the observation window of their respective patients. 
Thus, if you were to generate features similar to those you constructed in code/etl.py for the test data, all you have to do is
aggregate events for each patient.
IMPORTANT: Store your test data features in a file called "test_features.txt" where each line has the
patient_id followed by a space and the corresponding feature in sparse format.
Eg of a line:
60 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514
Here, 60 is the patient id and 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514 is the feature for the patient with id 60.

Save the file as "test_features.txt" and save it inside the folder deliverables

input:
output: X_train,Y_train,X_test
'''
def my_features():
	#TODO: complete this
    train_path = '../data/train/'
    events, mortality, feature_map = etl.read_csv(train_path)
    patient_features, mortality = etl.create_features(events, mortality, feature_map)
    etl.save_svmlight(patient_features, mortality, './tmp_svmlight.train', './features.train')
    X_train, Y_train = utils.get_data_from_svmlight("./tmp_svmlight.train")
    os.remove('./tmp_svmlight.train')
    os.remove('./features.train')

    
    test_path = '../data/test/'
    events = pd.read_csv(test_path + 'events.csv')
    feature_map = pd.read_csv(test_path + 'event_feature_map.csv')     
  
    indx = events.groupby("patient_id", as_index=False).max()[["patient_id", "timestamp"]]
    indx_date = indx.rename(columns={"timestamp":"indx_date"})
    
    df_merged = events.merge(indx_date, how="left", on="patient_id")
    filtered_events = df_merged[((pd.to_datetime(df_merged["indx_date"]) - pd.to_timedelta(2000,unit='d')).astype(str)<=df_merged["timestamp"])  & (df_merged["indx_date"]>=df_merged["timestamp"])]

    df = filtered_events.merge(feature_map, how="left", on="event_id")
    df = df[~df["value"].isnull()]    
    df_agg = df.groupby(["patient_id", "idx"], as_index=False).count()    
    df_agg = df_agg[["patient_id", "idx", "value"]].rename(columns={"idx":"feature_id", "value":"feature_value"})    
    max_features = df_agg.groupby("feature_id", as_index=False).max()[["feature_id", "feature_value"]].rename(columns={"feature_value":"feature_max"})
    df_max = df_agg.merge(max_features, how="inner", on="feature_id")
    df_max["feature_value"] = df_max["feature_value"] / df_max["feature_max"]    
    aggregated_events=df_max[["patient_id", "feature_id", "feature_value"]]
    
    patient_features = {}
    for x in range(0,len(aggregated_events)):
        row=aggregated_events.loc[x]
        if row["patient_id"] in patient_features:
            patient_features[row["patient_id"]].append((row["feature_id"], row["feature_value"]))
        else:
            patient_features[row["patient_id"]] = [tuple([row["feature_id"], row["feature_value"]])]
    
    op_file = './tmp_svmlight.test'
    file2 = open(op_file, 'wb+')
    for p_id in patient_features:
        label = 0
        line = str(label) + ' '        
        patient_features[p_id].sort()
        for tup in patient_features[p_id]:
            line = line + f'{tup[0]:.0f}' + f':{tup[1]:.6f} '
        file2.write(bytes(line + "\n", 'UTF-8'))
    file2.close()
        
    X_test, Y_test = utils.get_data_from_svmlight("./tmp_svmlight.test")
    os.remove('./tmp_svmlight.test')
    
    file = open('../deliverables/test_features.txt', 'w+')
    for p_id in patient_features:
        line = f'{p_id:.0f} '        
        patient_features[p_id].sort()
        for tup in patient_features[p_id]:
            line = line + f'{tup[0]:.0f}' + f':{tup[1]:.6f} '
        file.write(str(line + "\n"))   
    file.close()
    
    
    return X_train, Y_train, X_test


'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):
	#TODO: complete this
    clf = LogisticRegression(random_state=0)
    clf.fit(X_train, Y_train)
    Y_pred = clf.predict(X_test)
    return Y_pred

def my_classifier_prob(X_train, Y_train, X_test):
	#TODO: complete this
    #clf = RandomForestClassifier(random_state=0, max_depth=10)
    clf = LogisticRegression(random_state=0)
    #clf = LinearSVC(random_state=0)
    #clf = DecisionTreeClassifier(random_state = 0, max_depth=5)

    clf.fit(X_train, Y_train)
    Y_pred = clf.predict_proba(X_test)
    #print(clf.classes_)
    return Y_pred[:,1]

def main():
    X_train, Y_train, X_test = my_features()
    #Y_pred = my_classifier_predictions(X_train,Y_train,X_test)
    Y_pred_prob = my_classifier_prob(X_train,Y_train,X_test)
    #utils.generate_submission("../deliverables/test_features.txt",Y_pred)
    utils.generate_submission("../deliverables/test_features.txt",Y_pred_prob)
	#The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()

	