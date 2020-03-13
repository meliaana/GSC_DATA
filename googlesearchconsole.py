import pickle
import pandas as pd

from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SITE_URL = "https://climbitrange.co.uk/"



OAUTH_SCOPE = ('https://www.googleapis.com/auth/webmasters.readonly', 'https://www.googleapis.com/auth/webmasters')

# This is auth flow walks you through the Web auth flow the first time you run the script and stores the credentials in a file
# Every subsequent time you run the script, the script will use the "pickled" credentials stored in config/credentials.pickle
try:
    credentials = pickle.load(open("config/credentials.pickle", "rb"))
except (OSError, IOError,) as e:
    flow = InstalledAppFlow.from_client_secrets_file("client_id.json", scopes=OAUTH_SCOPE)
    credentials = flow.run_console()
    pickle.dump(credentials, open("config/credentials.pickle", "wb"))

# Connect to Search Console Service using the credentials
webmasters_service = build('webmasters', 'v3', credentials=credentials)

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'


def get_gsc_data(start,end):
    start_date = datetime(int(start[0:4]),int(start[5:7]),int(start[8:10]))
    end_date =  datetime(int(end[0:4]),int(end[5:7]),int(end[8:10]))
    print(start_date,end_date)
    maxRows = 25000
    i = 0
    output_rows = []
    date_2 = start_date - timedelta(days=1)
    while  date_2 < end_date :
        date_2 = date_2 + timedelta(days=1)    
        date = date_2.strftime("%Y-%m-%d")
        i = 0 
        output_rows.clear()
        while True:

            request = {
                'startDate' : date,
                'endDate' : date,
                'dimensions' : ["query","page","country","device"],
                "searchType": "Web",
                'rowLimit' : maxRows,
                'startRow' : i * maxRows

            }
        
            response = webmasters_service.searchanalytics().query(siteUrl = SITE_URL, body=request).execute()
            if response is None:
                #print("there is no response")
                break
            elif 'rows' not in response:
                #print("row not in response")
                break
            else:
                for row in response['rows']:
                    keyword = row['keys'][0]  #query
                    page = row['keys'][1]      
                    country = row['keys'][2]   
                    device = row['keys'][3]     
                    output_row = [date, keyword, page, country, device, row['clicks'], row['impressions'], row['ctr'], row['position']]
                    #print(output_row)
                    output_rows.append(output_row)
                i = i + 1
                df = pd.DataFrame(output_rows, columns=['date','query','page', 'country', 'device', 'clicks', 'impressions', 'ctr', 'avg_position'])
                file_name=str(date)+"_gsc_output.csv"
                df.to_csv("data_output/"+file_name)


get_gsc_data('2020-01-10','2020-01-20') #yyyy-mm-dd
