from HttpTriggerCallback import run_sql_command
# from MP4toMP3 import get_SAS_URL
import logging
import requests
import os
import azure.functions as func
from datetime import datetime
import azure.durable_functions as df
# from azure.storage.blob import BlockBlobService
from urllib.parse import quote
from urllib.parse import unquote

def post_mp3(
    URL,
    videoName
):
    # ## Create SAS URL
    # container = URL.split("/")[3]
    # sasURL = get_SAS_URL(
    #     fileURL=URL,
    #     block_blob_service=BlockBlobService(
    #         connection_string=os.getenv('fsevideosCS')
    #     ),
    #     container=container
    # )
    requestBody = {
        "contentUrls" : [URL],
        "properties" : {
             ## This means just one channel (either left or right, not sure) is processed,
             ##    this is to stop duplication as left and right channels are identical
            "channels" : [0]
        },
        "locale" : "en-GB",
        # "locale" : "en-AU",
        "displayName" : "This Is A Test Display Name"
    }
    logging.info(f"requestBody: {requestBody}")
    requestHeaders = {
        "Content-Type" : "application/json",
        "Ocp-Apim-Subscription-Key" : os.getenv("fse_speech_key")
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
    ## Extract the ID from it
    filesID = filesURL.split("/")[-2]
    ## Add to SQL with videoName
    q = f"""
INSERT INTO TranscriptFilesIDs (FilesID,VideoName)
VALUES ('{filesID}','{videoName.replace("'","''")}')
    """
    run_sql_command(
        sqlQuery=q,
        database="AzureCognitive"
    )


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    
    client = df.DurableOrchestrationClient(starter)

    # URL = quote(
    #     string=req.params.get('URL'),
    #     safe="/:"
    # )
    URL = req.params.get('URL')
    # videoName= req.params.get('videoName')
    logging.info(f"URL: {URL}")
    # logging.info(f"videoName: {videoName}")
    
    if URL.lower().endswith('.mp4'):
        instance_id = await client.start_new(
            orchestration_function_name="Orchestrator",
            instance_id=None,
            client_input={
                'fileURL' : URL
            }
        )
        return client.create_check_status_response(req, instance_id)
    elif URL.lower().endswith('.mp3'):
        blobName = URL.split("/")[-1]
        videoName = unquote(blobName)
        post_mp3(
            URL=URL,
            videoName=videoName
        )
        return 'done'


