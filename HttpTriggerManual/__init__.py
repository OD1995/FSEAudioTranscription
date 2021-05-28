import logging
import requests
import os
import azure.functions as func

def post_mp3(
    URL
):
    requestBody = {
        "contentUrls" : [URL],
        "properties" : {

        },
        "locale" : "en-UK",
        "displayName" : "This Is A Test Display Name"
    }
    requestHeaders = {
        "Content-Type" : "application/json",
        "Ocp-Apim-Subscription-Key" : os.getenv("fse_speech_key")
    }

    r = requests.post(
        url="https://uksouth.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions",
        data=requestBody,
        headers=requestHeaders
    )
    logging.info(f"r.headers: {r.headers}")
    logging.info(f"r.text:{r.text}")



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    URL = req.params.get('URL')
    logging.info(f"URL: {URL}")
    
    post_mp3(
        URL=URL
    )

    return func.HttpResponse("Success")
