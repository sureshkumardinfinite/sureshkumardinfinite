import csv,os
import gspread,pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('F:\intigral-bi\My First Project-9e7988effd6e.json', scope)
gc = gspread.authorize(credentials)

sheet=gc.open_by_url('https://docs.google.com/spreadsheets/d/1rEolWmdo4crytklGyHFBo0Lxv82zvMG6yjtoCFKtulg/edit#gid=1881938980')
print('Processing for work sheet -  Cookies')
worksheet_2 = sheet.worksheet("Cookies_ETL")

df_WS2 = pd.DataFrame(worksheet_2.get_all_records())
df_WS2.to_csv(os.path.join('F:\intigral-bi\wdir\marketing_spend_cookies\csv','Cookies_ETL.csv'),index=False)