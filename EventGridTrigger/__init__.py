import json
import logging

import azure.functions as func
import azure.durable_functions as df


async def main(event: func.EventGridEvent,
            starter: str):


	fileURL = event.get_json()['url']
	container = fileURL.split("/")[3]
	logging.info(f"fileURL: {fileURL}")
	logging.info(f"container: {container}")

	if fileURL.endswith(".mp4") & (container == "audiotranscript-files"):
		
		client = df.DurableOrchestrationClient(starter)

		instance_id = await client.start_new(
			orchestration_function_name="Orchestrator",
			instance_id=None,
			client_input={
				'fileURL' : fileURL
			}
		)
		logging.info(f"https://fseaudiotranscription.azurewebsites.net/runtime/webhooks/durabletask/instances/{instance_id}?taskHub=DurableFunctionsHub&connection=Storage&code=Xo4jdHQLMhoihVlz7Rjc9RmX6ElkOx3q7ySHnKMXQ1KM9/Zam1mGmw==")

	else:
		logging.info("not MP4 or not `audiotranscript-files` so no processing done")
