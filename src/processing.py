import pandas as pd
import numpy as np
from utils import Producer_custom, Consumer_custom
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

bootstrap_server_consume = 'localhost:9094'
topic_consume = ['raw_data']
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'data_processors'}
consumer = Consumer_custom(conf_consume)
consumer.subscribe(topic_consume)

bootstrap_server_produce = 'localhost:9097'
topic_produce = 'processed_data'
conf_produce = {'bootstrap.servers': bootstrap_server_produce}
producer = Producer_custom(conf_produce)

while True:
    data, _ = consumer.get_message(timeout=1000)
    if data is not None:
        data = pd.DataFrame(data)
        data['Start_Time'] = pd.to_datetime(data['Start_Time'], errors='coerce')
        data['End_Time'] = pd.to_datetime(data['End_Time'], errors='coerce')

        data['Time_Duration(min)']=round((data['End_Time']-data['Start_Time'])/np.timedelta64(1,'m'))
        data.dropna(subset=['Time_Duration(min)'],axis=0,inplace=True)
        median = data['Time_Duration(min)'].median()
        std = data['Time_Duration(min)'].std()
        outliers = (data['Time_Duration(min)'] - median).abs() > std*3
        data[outliers] = np.nan
        data['Time_Duration(min)'].fillna(median, inplace=True)
        feature_lst=['Source','Severity','Start_Lng','Start_Lat','Distance(mi)','City','County','State', 'Time_Duration(min)']
        data_sel=data[feature_lst].copy()
        data_sel.isnull().mean()
        data_sel.dropna(subset=data_sel.columns[data_sel.isnull().mean()!=0], how='any', axis=0, inplace=True)
        data_state=data_sel.loc[data_sel.State=='PA'].copy()
        data_state.drop('State',axis=1, inplace=True)
        data_county=data_state.loc[data_state.County=='Montgomery'].copy()
        data_county.drop('County',axis=1, inplace=True)
        data_county_dummy = pd.get_dummies(data_county, drop_first=True)

        data_y = data_county_dummy['Severity'].to_numpy().tolist()
        data_X = data_county_dummy.drop('Severity', axis=1).to_numpy().tolist()

        data = {"data_y": data_y, "data_X": data_X}
        f = list(set(data_y))
        flag = True
        for i in range(len(f)):
            if data_y.count(f[i]) < 3:
                flag = False
        if flag != False:
            producer.send_message(topic_produce, key='1', value=data)
            print(f"Producer из processing.py отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data)}")