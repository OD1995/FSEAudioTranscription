import requests
from datetime import datetime
import sys
sys.path.insert(0,r"K:\Technology Team\Python")
from sql_helper import fromSQLquery
from time import sleep

Q = "SELECT * FROM [TranscriptFilesIDs] WHERE [Status] IS NULL"
df = fromSQLquery(query=Q,
                 servername='inf',
                 database="AzureCognitive")

start_url = 'https://uksouth.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions'

for filesID in df.FilesID[1:]:
    print(datetime.now())
    print(filesID)
    r = requests.post(
        'https://fseaudiotranscription.azurewebsites.net/api/HttpTriggerCallback',
        json={
            'self' : f'{start_url}/{filesID}'
        }
    )
    print(r)
    sleep(20)