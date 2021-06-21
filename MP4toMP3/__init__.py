# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import json
import logging
from moviepy.editor import VideoFileClip
from moviepy.config import FFMPEG_BINARY
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
from urllib.parse import unquote
import azure.functions as func
import os
from azure.storage.blob import BlockBlobService

def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
        container_name=container,
        permission=ContainerPermissions.READ,
        expiry=datetime.utcnow() + timedelta(days=7)
    )
    return f"{fileURL.replace(' ','%20')}?{sasTokenRead}"

def main(activityInput: dict, msg: func.Out[str]) -> str:

    container = activityInput['fileURL'].split("/")[3]

    logging.info(f"URL: {activityInput['fileURL']}")

    ## Create SAS URL
    bbs = BlockBlobService(
        connection_string=os.getenv('fsevideosCS')
    )
    sasURL = get_SAS_URL(
        fileURL=activityInput['fileURL'],
        block_blob_service=bbs,
        container=container
    )
    logging.info(f"sasURL: {sasURL}")
    ## Get MP4 in VideoFileClip object
    logging.info(f"FFMPEG_BINARY: {FFMPEG_BINARY}")
    clip = VideoFileClip(sasURL)
    logging.info(f"clip.duration: {clip.duration}")
    ## Create path to save 
    blobName = activityInput['fileURL'].split("/")[-1]
    mp3Name = unquote(blobName.replace(".mp4",".mp3"))
    tempClipFilePath = "/tmp/" + mp3Name
    ## Save audio to path
    clip.audio.write_audiofile(tempClipFilePath)
    logging.info(f"blob saved to: {tempClipFilePath}")
    ## Upload blob to the same container
    bbs.create_blob_from_path(
        container_name=container,
        blob_name=mp3Name,
        file_path=tempClipFilePath
    )
    mp3URL = f"https://fsevideos.blob.core.windows.net/{container}/{mp3Name}"
    logging.info(f"blob created: {mp3URL}")
    ## Add message to queue
    msgDict = {
        'URL' : get_SAS_URL(
            fileURL=mp3URL,
            block_blob_service=bbs,
            container=container
        ),
        'videoName' : mp3Name
    }
    msgStr = json.dumps(msgDict)
    logging.info(f"msgStr: {msgStr}")
    msg.set(msgStr)

    return "Done"
