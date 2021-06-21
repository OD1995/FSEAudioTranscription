import logging
import requests
import azure.functions as func
import os


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
THIS DOES NOT WORK, NEXT TIME A NEW WEB HOOK IS NEEDED, TRY TO WORK OUT WHY
    """
    
    logging.info('Python HTTP trigger function processed a request.')



    callbackURL = "https://fseaudiotranscription.azurewebsites.net/api/HttpTriggerCallback"

    r = requests.post(
        url="https://uksouth.api.cognitive.microsoft.com/speechtotext/v3.0/webhooks",
        headers={
            "Content-Type" : 'application/json',
            'Ocp-Apim-Subscription-Key' : os.getenv("fse_speech_key")
        },
        data={
            "displayName": "TranscriptionCompletionWebHook",
            "webUrl": callbackURL,
            "events": {
                "transcriptionCompletion": True
            },
            "description": "I registered this URL to get a POST request for each completed transcription."

        }
    )

    logging.info(r.json())

    webhooksURL = 'https://fseaudiotranscription.azurewebsites.net/api/HttpTriggerCheckWebHooks'
    return func.HttpResponse(
        body=f"WebHook post request created, go to `{webhooksURL}` to check if it worked"
    )

