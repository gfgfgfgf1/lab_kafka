import pandas as pd
import random
import time
from utils import Producer_custom

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


bootstrap_server_produce = 'localhost:9094'
topic_produce = 'raw_data'
conf_produce = {'bootstrap.servers': bootstrap_server_produce}
producer_1 = Producer_custom(conf_produce)
producer_2 = Producer_custom(conf_produce)


dataset = pd.read_csv(f"./data/US_Accidents_March23_sampled_500k.csv")
feature_lst=['Source','Start_Time','End_Time','Severity','Start_Lng','Start_Lat','Distance(mi)','City','County','State']
data_sel=dataset[feature_lst].copy()

while True:
    data_1 = data_sel.sample(frac=random.uniform(0.001, 0.007)).to_dict()
    data_2 = data_sel.sample(frac=random.uniform(0.001, 0.007)).to_dict()
    producer_1.send_message(topic_produce, key='1', value=data_1)
    producer_2.send_message(topic_produce, key='1', value=data_2)

    print(f"Producer 1 из generation.py отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data_1)}")
    print(f"Producer 2 из generation.py отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data_2)}")
    time.sleep(50 + random.uniform(-5.0, 5.0))