import utils
import pandas as pd

# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

def read_csv(filepath):
    
    '''
    TODO: This function needs to be completed.
    Read the events.csv, mortality_events.csv and event_feature_map.csv files into events, mortality and feature_map.
    
    Return events, mortality and feature_map
    '''

    #Columns in events.csv - patient_id,event_id,event_description,timestamp,value
    events = pd.read_csv(filepath + 'events.csv')
    
    #Columns in mortality_event.csv - patient_id,timestamp,label
    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    #Columns in event_feature_map.csv - idx,event_id
    feature_map = pd.read_csv(filepath + 'event_feature_map.csv')

    return events, mortality, feature_map


def calculate_index_date(events, mortality, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Create list of patients alive ( mortality_events.csv only contains information about patients deceased)
    2. Split events into two groups based on whether the patient is alive or deceased
    3. Calculate index date for each patient
    
    IMPORTANT:
    Save indx_date to a csv file in the deliverables folder named as etl_index_dates.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, indx_date.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    Return indx_date
    '''
    df_merge = events.merge(mortality,how="left", on="patient_id" )
    df_alive = df_merge[df_merge["label"].isnull()]
    df_dead = df_merge[df_merge["label"]==1]
    
    dead_indexes = df_dead.groupby("patient_id", as_index=False).max()[["patient_id","timestamp_y"]].rename(columns={"timestamp_y":"indx_date"})
    dead_indexes["indx_date"] = pd.to_datetime(dead_indexes["indx_date"])-pd.to_timedelta(30,unit='d')
    dead_indexes["indx_date"] = dead_indexes["indx_date"].astype(str)
    
    alive_indexes = df_alive.groupby("patient_id", as_index=False).max()[["patient_id", "timestamp_x"]]
    alive_indexes = alive_indexes.rename(columns={"timestamp_x":"indx_date"})


    indx_date = pd.concat([dead_indexes, alive_indexes])
    indx_date.to_csv(deliverables_path + "etl_index_dates.csv", index=False)
    return indx_date


def filter_events(events, indx_date, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 b

    Suggested steps:
    1. Join indx_date with events on patient_id
    2. Filter events occuring in the observation window(IndexDate-2000 to IndexDate)
    
    
    IMPORTANT:
    Save filtered_events to a csv file in the deliverables folder named as etl_filtered_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    Return filtered_events
    '''
    df_merged = events.merge(indx_date, how="left", on="patient_id")
    filtered_events = df_merged[((pd.to_datetime(df_merged["indx_date"]) - pd.to_timedelta(2000,unit='d')).astype(str)<=df_merged["timestamp"])  & (df_merged["indx_date"]>=df_merged["timestamp"])]

    filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)
    return filtered_events


def aggregate_events(filtered_events_df, mortality_df,feature_map_df, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 c

    Suggested steps:
    1. Replace event_id's with index available in event_feature_map.csv
    2. Remove events with n/a values
    3. Aggregate events using sum and count to calculate feature value
    4. Normalize the values obtained above using min-max normalization(the min value will be 0 in all scenarios)
    
    
    IMPORTANT:
    Save aggregated_events to a csv file in the deliverables folder named as etl_aggregated_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header .
    For example if you are using Pandas, you could write: 
        aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    Return filtered_events
    '''
    df = filtered_events_df.merge(feature_map_df, how="left", on="event_id")
    df = df[~df["value"].isnull()]
    
    df_agg = df.groupby(["patient_id", "idx"], as_index=False).count()    
    df_agg = df_agg[["patient_id", "idx", "value"]].rename(columns={"idx":"feature_id", "value":"feature_value"})
    
    max_features = df_agg.groupby("feature_id", as_index=False).max()[["feature_id", "feature_value"]].rename(columns={"feature_value":"feature_max"})
    df_max = df_agg.merge(max_features, how="inner", on="feature_id")
    df_max["feature_value"] = df_max["feature_value"] / df_max["feature_max"]
    
    aggregated_events=df_max[["patient_id", "feature_id", "feature_value"]]
    aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)
    return aggregated_events

def create_features(events, mortality, feature_map):
    
    deliverables_path = '../deliverables/'

    #Calculate index date
    indx_date = calculate_index_date(events, mortality, deliverables_path)

    #Filter events in the observation window
    filtered_events = filter_events(events, indx_date,  deliverables_path)
    
    #Aggregate the event values for each patient 
    aggregated_events = aggregate_events(filtered_events, mortality, feature_map, deliverables_path)

    '''
    TODO: Complete the code below by creating two dictionaries - 
    1. patient_features :  Key - patient_id and value is array of tuples(feature_id, feature_value)
    2. mortality : Key - patient_id and value is mortality label
    '''
    patient_features = {}
    for x in range(0,len(aggregated_events)):
        row=aggregated_events.loc[x]
        if row["patient_id"] in patient_features:
            patient_features[row["patient_id"]].append((row["feature_id"], row["feature_value"]))
        else:
            patient_features[row["patient_id"]] = [tuple([row["feature_id"], row["feature_value"]])]
    
    mortal_df = events[["patient_id"]].drop_duplicates().merge(mortality, how="left", on="patient_id")
    #mortal_df.loc[mortal_df["label"]==1, "label"] = 0
    mortal_df.loc[mortal_df["label"].isnull(), "label"] = 0 
    mortality = {}
    for x in range(0,len(mortal_df)):
        row = mortal_df.loc[x]
        mortality[row["patient_id"]] = row["label"]        


    return patient_features, mortality

def save_svmlight(patient_features, mortality, op_file, op_deliverable):
    
    '''
    TODO: This function needs to be completed

    Refer to instructions in Q3 d

    Create two files:
    1. op_file - which saves the features in svmlight format. (See instructions in Q3d for detailed explanation)
    2. op_deliverable - which saves the features in following format:
       patient_id1 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...
       patient_id2 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...  
    
    Note: Please make sure the features are ordered in ascending order, and patients are stored in ascending order as well.     
    '''    
    
    deliverable1 = open(op_file, 'wb')
    deliverable2 = open(op_deliverable, 'wb')
    for p_id in patient_features:
        label = mortality[p_id]
        line = str(label) + ' '
        
        patient_features[p_id].sort()
        for tup in patient_features[p_id]:
            line = line + f'{tup[0]:.0f}' + f':{tup[1]:.6f} '
        deliverable1.write(bytes(line + "\n", 'UTF-8'))
        deliverable2.write(bytes(f'{p_id:.0f} ' + line+"\n", 'UTF-8'))
    deliverable1.close()
    deliverable2.close()
    
    #deliverable1.write(bytes((""),'UTF-8')); #Use 'UTF-8'
    #deliverable2.write(bytes((""),'UTF-8'));

def main():
    train_path = '../data/train/'
    events, mortality, feature_map = read_csv(train_path)
    patient_features, mortality = create_features(events, mortality, feature_map)
    save_svmlight(patient_features, mortality, '../deliverables/features_svmlight.train', '../deliverables/features.train')

if __name__ == "__main__":
    main()