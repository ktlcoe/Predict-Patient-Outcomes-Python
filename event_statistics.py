import time
import pandas as pd
import numpy as np

# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

def read_csv(filepath):
    '''
    TODO : This function needs to be completed.
    Read the events.csv and mortality_events.csv files. 
    Variables returned from this function are passed as input to the metric functions.
    '''
    events = pd.read_csv(filepath + 'events.csv')
    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    return events, mortality

def event_count_metrics(events, mortality):
    '''
    TODO : Implement this function to return the event count metrics.
    Event count is defined as the number of events recorded for a given patient.
    '''
    df_merge = events.merge(mortality,how="left", on="patient_id" )
    df_alive = df_merge[df_merge["label"].isnull()]
    df_dead = df_merge[df_merge["label"]==1]
    df_alive_group = df_alive.groupby(["patient_id"]).count()
    df_dead_group = df_dead.groupby(["patient_id"]).count()

    
    avg_dead_event_count = df_dead_group["event_id"].mean()
    max_dead_event_count = df_dead_group["event_id"].max()
    min_dead_event_count = df_dead_group["event_id"].min()
    avg_alive_event_count = df_alive_group["event_id"].mean()
    max_alive_event_count = df_alive_group["event_id"].max()
    min_alive_event_count = df_alive_group["event_id"].min()

    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count

def encounter_count_metrics(events, mortality):
    '''
    TODO : Implement this function to return the encounter count metrics.
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU. 
    '''
    df_merge = events.merge(mortality,how="left", on="patient_id" )
    df_alive = df_merge[df_merge["label"].isnull()]
    df_dead = df_merge[df_merge["label"]==1]
    df_alive_group = df_alive.groupby(["patient_id"]).nunique()
    df_dead_group= df_dead.groupby(["patient_id"]).nunique() 
    
    
    avg_dead_encounter_count = df_dead_group["timestamp_x"].mean()
    max_dead_encounter_count = df_dead_group["timestamp_x"].max()
    min_dead_encounter_count = df_dead_group["timestamp_x"].min()
    avg_alive_encounter_count = df_alive_group["timestamp_x"].mean()
    max_alive_encounter_count = df_alive_group["timestamp_x"].max()
    min_alive_encounter_count = df_alive_group["timestamp_x"].min()

    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count

def record_length_metrics(events, mortality):
    '''
    TODO: Implement this function to return the record length metrics.
    Record length is the duration between the first event and the last event for a given patient. 
    '''
    df_merge = events.merge(mortality,how="left", on="patient_id" )
    df_alive = df_merge[df_merge["label"].isnull()]
    df_dead = df_merge[df_merge["label"]==1]    
    df_alive_times = df_alive.groupby("patient_id").max().merge(df_alive.groupby("patient_id").min(), how="inner", on="patient_id")
    df_dead_times = df_dead.groupby("patient_id").max().merge(df_dead.groupby("patient_id").min(), how="inner", on="patient_id")
    
    alive_times = pd.Series(pd.to_datetime(df_alive_times["timestamp_x_x"]) - pd.to_datetime(df_alive_times["timestamp_x_y"])).dt.days
    dead_times = pd.Series(pd.to_datetime(df_dead_times["timestamp_x_x"]) - pd.to_datetime(df_dead_times["timestamp_x_y"])).dt.days
    
    avg_dead_rec_len = dead_times.mean()
    max_dead_rec_len = dead_times.max()
    min_dead_rec_len = dead_times.min()
    avg_alive_rec_len = alive_times.mean()
    max_alive_rec_len = alive_times.max()
    min_alive_rec_len = alive_times.min()

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len

def main():
    '''
    DO NOT MODIFY THIS FUNCTION.
    '''
    # You may change the following path variable in coding but switch it back when submission.
    train_path = '../data/train/'

    # DO NOT CHANGE ANYTHING BELOW THIS ----------------------------
    events, mortality = read_csv(train_path)

    #Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print(("Time to compute event count metrics: " + str(end_time - start_time) + "s"))
    print(event_count)

    #Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print(("Time to compute encounter count metrics: " + str(end_time - start_time) + "s"))
    print(encounter_count)

    #Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print(("Time to compute record length metrics: " + str(end_time - start_time) + "s"))
    print(record_length)
    
if __name__ == "__main__":
    main()
