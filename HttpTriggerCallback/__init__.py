import logging
from datetime import datetime
import pandas as pd
import pyodbc
import azure.functions as func
import requests
import os


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

def sql_query_to_df(
    sqlQuery,
    database
):
    with pyodbc.connect(get_connection_string(database)) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(
            sql=sqlQuery,
            con=conn
        )
    return df
            
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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    logging.info("params")
    logging.info(req.params)

    if "validationToken" in req.params:
        return func.HttpResponse(
        status_code=200,
        body=req.params.get('validationToken')
        )

    try:
        logging.info("headers")
        logging.info(req.headers())
    except:
        logging.info("that didn't work")
    try:
        logging.info("get_json")
        js = req.get_json()
        logging.info(js)
    except:
        logging.info("that didn't work either")

    ## Get files
    requestHeaders = {
        "Content-Type" : "application/json",
        "Ocp-Apim-Subscription-Key" : os.getenv("fse_speech_key")
    }
    filesURL = f"{js['self']}/files"
    filesID = filesURL.split("/")[-2]
    logging.info(f"filesURL: {filesURL}")
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
    ## Get VideoName from SQL
    Q = f"""
SELECT VideoName
FROM TranscriptFilesIDs
WHERE FilesID = '{filesID}'
ORDER BY DateTimeRowAdded DESC
    """
    sqlDF = sql_query_to_df(
        sqlQuery=Q,
        database="AzureCognitive"
    )
    ## Choose most recent row (in the unlikely event of duplication)
    videoName = sqlDF.loc[0,'VideoName']
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
    ## Split df into blocks of 900 rows (1000 row max upload)
    n = 900
    indexBlocks = [
        list(df.index)[i * n:(i + 1) * n]
        for i in range((len(list(df.index)) + n - 1) // n )]
    for indexBlock in indexBlocks:
        DF = df[df.index.isin(indexBlock)]
        q = create_insert_query(
            df=DF,
            columnDict=columnDict,
            sqlTableName="VideoIndexerTranscripts"
        )

        run_sql_command(
            sqlQuery=q,
            database="AzureCognitive"
        )

    return func.HttpResponse(
        status_code=200
    )