import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize
import requests

url =  ''
response = requests.get(url)
content = response.content.decode('utf-8') # list of ugly strings
j = json.loads(content) # json list having nested dictionary

df_a = json_normalize(data=j['channels'])
df_channelImages = json_normalize(data=j['channels'], record_path=["channelImages"], record_prefix="channelImages.")
df_subscriptions = json_normalize(data=j['channels'], record_path=["subscriptions"], record_prefix="subscriptions.")
df_disabledDevices = json_normalize(data=j['channels'], record_path=["disabledDevices"], record_prefix="disabledDevices.")

df = pd.concat([df_a,df_channelImages,df_subscriptions,df_disabledDevices], axis=1)
del df['channelImages']
del df['disabledDevices']
del df['subscriptions']
df.to_csv("finally.csv",index=False)

