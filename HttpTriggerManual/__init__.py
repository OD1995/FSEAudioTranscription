import logging
import requests
import os
import azure.functions as func
from datetime import date, datetime
import pandas as pd
import pyodbc

def get_best(L):
    ## Sort all options based on `confidence` descending
    S = sorted(L, key=lambda k: k['confidence'],reverse=True)
    return S[0]

def get_connection_string(database):
    username = 'matt.shepherd'
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    server = "fse-inf-live-uk.database.windows.net"
    # database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    return connectionString

def run_sql_command(
    sqlQuery,
    database
):
    ## Create connection string
    connectionString = get_connection_string(database)
    ## Run query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlQuery)

def get_results_to_df(
    url,
    videoName
):
    rows = []
    r3 = requests.get(url)
    logging.info(datetime.now())

    js = r3.json()

    tickDenom = 10000000
    for phrase in js['recognizedPhrases']:
        
        tba = {}
        
        tba['VideoName'] = videoName
        # tba['OriginalVideoName'] = videoName
        
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
        
        rows.append(tba)

    df = pd.DataFrame(rows)

    return df

def create_insert_query(df,columnDict,sqlTableName):
    """
    Inputs: - df - pandas.DataFrame
            - columnDict - dict - keys - column names
                                - vals - column (rough) SQL data types (as strings)
            - sqlTableName - str
    Output: - SQL insert query - str
    """
    ## Create column list string
    columnsListStr = "[" + "],[".join(columnDict.keys()) + "]"
    ## Convert df into a string of rows to upload
    stringRows = rows_to_strings(df,columnDict)
    
    
    Q = f"""
INSERT INTO {sqlTableName}
({columnsListStr})
VALUES {','.join(stringRows)}
    """
    return Q

def rows_to_strings(df,columnDict):
    
    listToReturn = []
    
    ## Loop throw the rows (as dicts)
    for row_dict in df.to_dict(orient="records"):
        ## Create list of strings formatted in the way SQL expects them
        ##    based on their SQL data type
        rowList = [sqlise(
                    _val_=row_dict[colName],
                    _format_=colType
                            )
                    for colName,colType in columnDict.items()]
        ## Create SQL ready string out of the list
        stringRow = "\n(" + ",".join(rowList) + ")"
        ## Add string to list
        listToReturn.append(stringRow)
        
    return listToReturn

            
def sqlise(_val_,_format_):
    if _val_ is None:
        return "NULL"
    elif _format_ == "str":
        return "'" + str(_val_).replace("'","''") + "'"
    elif _format_ == "DateTime":
        ## datetime gives 6 microsecond DPs, SQL only takes 3
        return "'" + _val_.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "'"
    elif _format_ == "float":
        return str(_val_)
    else:
        raise ValueError(f"Data type `{_format_}` not expected")

def post_mp3(
    URL,
    videoName
):
    requestBody = {
        "contentUrls" : [URL],
        "properties" : {
             ## This means just one channel (either left or right, not sure) is processed,
             ##    this is to stop duplication as left and right channels are identical
            "channel" : [0]
        },
        "locale" : "en-GB",
        "displayName" : "This Is A Test Display Name"
    }
    requestHeaders = {
        "Content-Type" : "application/json",
        # "Ocp-Apim-Subscription-Key" : os.getenv("fse_speech_key"),
        "Ocp-Apim-Subscription-Key" : "18059fa0243648f683d22fb2933592b2"
    }
    ## Create transcription
    logging.info(datetime.now())
    r1 = requests.post(
        url="https://uksouth.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions",
        json=requestBody,
        headers=requestHeaders
    )
    logging.info(datetime.now())
    logging.info(f"r1.headers: {r1.headers}")
    logging.info(f"r1.text: {r1.text}")
    ## Get the files URL
    filesURL = r1.json()['links']['files']
    ## Get files
    r2 = requests.get(
        url=filesURL,
        headers=requestHeaders
    )
    logging.info(datetime.now())
    logging.info(f"r2.headers: {r2.headers}")
    logging.info(f"r2.text: {r2.text}")
    ## Extract relevant URL
    transcriptURL = [
        x
        for x in r2.json()['values']
        if x['kind'] == 'Transcription'
    ][0]['links']['contentUrl']
    ## Get transcription in df
    df = get_results_to_df(
        url=transcriptURL,
        videoName=videoName
    )
    logging.info(f"df.shape: {df.shape}")
    columnDict = {
        'VideoName' : 'str',
        'DateTimeAdded' : 'DateTime',
        'Accuracy' : 'float',
        'Text' : 'str',
        'TextStartTime' : 'str',
        'TextEndTime' : 'str'
    }

    q = create_insert_query(
        df=df,
        columnDict=columnDict,
        sqlTableName="VideoIndexerTranscripts"
    )

    run_sql_command(
        sqlQuery=q,
        database="AzureCognitive"
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    URL = req.params.get('URL')
    logging.info(f"URL: {URL}")
    
    post_mp3(
        URL=URL
    )

    return func.HttpResponse("Success")
