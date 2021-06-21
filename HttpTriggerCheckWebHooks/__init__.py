import logging
import requests
import azure.functions as func
import os
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    r = requests.get(
        url="https://uksouth.api.cognitive.microsoft.com/speechtotext/v3.0/webhooks",
        headers={
            "Ocp-Apim-Subscription-Key" : os.getenv("fse_speech_key")
        }
    )

    return func.HttpResponse(
        json.dumps(r.json()),
        mimetype="application/json",
    )
