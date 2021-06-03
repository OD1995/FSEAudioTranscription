import json
import logging
from HttpTriggerManual import post_mp3
import azure.functions as func


def main(msg: func.QueueMessage) -> None:
    msgDict = json.loads(
        msg.get_body().decode('utf-8')
    )

    logging.info(f"URL: {msgDict['URL']}")
    logging.info(f"videoName: {msgDict['videoName']}")
    
    post_mp3(
        URL=msgDict['URL'],
        videoName=msgDict['videoName']
    )
                 
