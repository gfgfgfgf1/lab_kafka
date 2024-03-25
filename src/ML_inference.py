from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
import pandas as pd
from utils import Producer_custom, Consumer_custom

bootstrap_server_consume = 'localhost:9097'
topic_consume = ['processed_data']
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'ML_inference'}
consumer = Consumer_custom(conf_consume)
consumer.subscribe(topic_consume)

bootstrap_server_produce = 'localhost:9094'
topic_produce = 'ML_results'
conf_produce = {'bootstrap.servers': bootstrap_server_produce}
producer = Producer_custom(conf_produce)

while True:
    data, _ = consumer.get_message(timeout=1000)
    if data is not None:
        data_x = pd.DataFrame(data['data_X'])
        data_y = pd.DataFrame(data['data_y'])
        X_train, X_test, y_train, y_test = train_test_split(data_x, data_y, test_size=0.2, random_state=21, stratify=data_y)
        lr = LogisticRegression(random_state=0)
        lr.fit(X_train, y_train)
        y_pred=lr.predict(X_test)
        accuracy_log_reg=accuracy_score(y_test, y_pred)

        clf=RandomForestClassifier(n_estimators=100)
        clf.fit(X_train, y_train)
        y_pred=clf.predict(X_test)
        accuracy_random_forest=accuracy_score(y_test, y_pred)

        results = {"LogisticRegression": accuracy_log_reg, "RandomForestClassifier": accuracy_random_forest}
        producer.send_message(topic_produce, key='1', value=results)

        print(f"Producer из ML_inference.py отправил данные в topic '{topic_produce}':\n {results}")