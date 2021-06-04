import logging
import azure.durable_functions as df
from urllib.parse import quote

import azure.functions as func


async def main(myblob: func.InputStream, starter: str):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes"
                 f"Blob URI: `{myblob.uri}`")

    if myblob.uri.endswith(".mp4"):
        client = df.DurableOrchestrationClient(starter)

        URL = quote(
            string=myblob.uri,
            safe="/:"
        )
        # videoName= req.params.get('videoName')
        logging.info(f"URL: {URL}")
        # logging.info(f"videoName: {videoName}")
        
        # post_mp3(
        #     URL=URL,
        #     videoName=videoName
        # )

        instance_id = await client.start_new(
            orchestration_function_name="Orchestrator",
            instance_id=None,
            client_input={
                'fileURL' : URL
            }
        )
        logging.info(f"https://fseaudiotranscription.azurewebsites.net/runtime/webhooks/durabletask/instances/{instance_id}?taskHub=DurableFunctionsHub&connection=Storage&code=Xo4jdHQLMhoihVlz7Rjc9RmX6ElkOx3q7ySHnKMXQ1KM9/Zam1mGmw==")

    else:
        logging.info("not MP4 so no processing done")