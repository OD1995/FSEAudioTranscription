import requests
from datetime import datetime
import pandas as pd

def get_best(L):
    ## Sort all options based on `confidence` descending
    S = sorted(L, key=lambda k: k['confidence'],reverse=True)
    return S[0]

rows = []

url = "https://spsvcproduks.blob.core.windows.net/bestor-948e9f4b-98f0-414a-b695-603be7bddabe/TranscriptionData%2F65dfa066-87f1-449d-aeac-f9af90b068c2_0_0.json?sv=2020-04-08&st=2021-06-01T14%3A54%3A37Z&se=2021-06-02T02%3A59%3A37Z&sr=b&sp=rl&sig=I0f8c6%2FmTHFe3z6bg3E89d%2FNEdBCYAJ7ClicOlTOvQ4%3D"

r = requests.get(url)

js = r.json()

tickDenom = 10000000
for phrase in js['recognizedPhrases']:
    
    tba = {}
    
    tba['VideoName'] = "" # FILL ME
    
    tba['DateTimeAdded'] = datetime.strptime(
        js['timestamp'],
        "%Y-%m-%dT%H:%M:%SZ"
    )

    tba['TextStartTime'] = datetime.fromtimestamp(
        phrase['offsetInTicks']/tickDenom
    ).strftime('%H:%M:%S.%f')[:-3]

    tba['TextEndTime'] = datetime.fromtimestamp(
        (phrase['offsetInTicks'] + phrase['durationInTicks'])/tickDenom
    ).strftime('%H:%M:%S.%f')[:-3]

    best = get_best(phrase['nBest'])

    tba['Accuracy'] = best['confidence']
    tba['Text'] = best['display']
    
#    print(tba)

    rows.append(tba)

df = pd.DataFrame(rows)










