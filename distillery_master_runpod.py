#### Distillery Master v2.4 - Aug 24 2023 - From pop GenerationQueue to push SendQueue
####
#### For use in Distillery Discord Server. This script: 
####    (a) runs a loop that pops the requests from GenerationQueue;
####    (b) sends the requests to Runpod;
####    (c) receives the outputs from Runpod and saves the results as png files;
####    (d) pushes the result (including a list of urls to the output files) to the SendQueue table for subsequent delivery.
####                                                                                                                                                                                            ./ >:""

import os
import asyncio
import json
import signal
import runpod
from runpod import AsyncioEndpoint, AsyncioJob
import base64
import io
from PIL import Image, PngImagePlugin
import signal
from contextlib import asynccontextmanager
import distillery_aws
import uuid
import time
import random
from distillery_commands import InputPreprocessor
import copy
import sys
import aiohttp

######## Inputs below this line
try:
    CONFIG = json.load(open('config/config.json'))
except Exception as e:
    print("Failed to load config.json file. Please make sure it exists and is valid. Error:", e)
    exit(1)
APP_NAME = CONFIG['APP_NAME']
SECONDS_PER_TICK = CONFIG['SECONDS_PER_TICK'] # Number of seconds between each tick of the loop
MAX_GENERATIONQUEUE_POP_COUNT=int(CONFIG['MAX_GENERATIONQUEUE_POP_COUNT']) # Maximum number of requests to pop from GenerationQueue at any given time
SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT=int(CONFIG['SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT']) # Multiplier for SECONDS_PER_TICK for each request popped from GenerationQueue
RUNPOD_KEY = os.getenv('RUNPOD_API_KEY')  # Fetch token from environment variable; add this to the environment variables of your system.
######## End of inputs; no other inputs are made below this line
INSTANCE_IDENTIFIER = APP_NAME+ '-' + str(uuid.uuid4()) # Unique identifier for this instance of the Master
runpod.api_key=RUNPOD_KEY
MAX_RUNPOD_ATTEMPTS = 3 # Maximum number of attempts to call Runpod
generationqueue_pop_counter = 0 # Counter for the number of requests popped from GenerationQueue

# 1. Support functions
def set_tick_time(generationqueue_pop_counter): # Set the tick time based on the number of requests popped from GenerationQueue
    tick_time = SECONDS_PER_TICK*(1+SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT*generationqueue_pop_counter) 
    return tick_time

def flatten_list(nested_list):
    flat_list = []
    for item in nested_list:
        if isinstance(item, list):
            flat_list.extend(flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list

# 2. Image generation functions
async def call_runpod(request_id, payload, command_args):  # Sends the request to the Runpod API and returns the image_list
    for attempt in range(MAX_RUNPOD_ATTEMPTS):
        try:
            aws_manager = await distillery_aws.AWSManager.get_instance()
            if isinstance(payload, str):
                payload = json.loads(payload)
            async with aiohttp.ClientSession() as session:
                endpoint = AsyncioEndpoint(command_args['ENDPOINT_ID'], session)
                job: AsyncioJob = await endpoint.run(payload)
                status = await job.status()
                aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Runpod Status for request_id {request_id}: {status}", level="INFO")
                output = await job.output()
                aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Runpod Output for request_id {request_id}: {output}", level="INFO")
            print("--------------------------------------------")
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Runpod called successfully. Output: {output}", level='INFO')  # Print the output for debugging
            return output
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no} (attempt {attempt+1}): {str(e)}'
            print(INSTANCE_IDENTIFIER + " - call_runpod - " + error_message)
            if attempt < 2:
                aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='WARNING')
            else:
                aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='ERROR')

# 3. Creation functions that will be running in loop
async def create_routine(tuple): # Receives a tuple popped from GenerationQueue, calls Runpod, and returns the image URLs
    global generationqueue_pop_counter
    try:
        aws_manager = await distillery_aws.AWSManager.get_instance()
        request_id = tuple[0]
        username = tuple[1]
        generation_input_timestamp = tuple[2]
        payload = tuple[3]
        generation_command_args = tuple[4]
        message_data = tuple[5]
        generation_other_data = tuple[6]
        generation_output_timespentingenerationqueue=tuple[7]
        total_batches = generation_command_args['TOTAL_BATCHES']
        image_urls = []
        starting_seed = int(payload['template_inputs']['NOISE_SEED'])
        payload_template_key = payload['template_inputs']['PAYLOAD_TEMPLATE_KEY']
        payload_template_map = json.load(open(CONFIG['COMFY_TEMPLATE_MAP'])) 
        async def fetch_image(i):
            local_payload = copy.deepcopy(payload)
            new_seed = starting_seed + i * int(generation_command_args['IMG_PER_BATCH'])
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Batch {i+1} of {total_batches} - Increment by: {i * generation_command_args['IMG_PER_BATCH']}, New Seed: {new_seed}, Starting Seed: {starting_seed}", level='INFO')
            print(f"Batch {i+1} of {total_batches} - Increment by: {i * generation_command_args['IMG_PER_BATCH']}, New Seed: {new_seed}, Starting Seed: {starting_seed}")
            local_payload['comfy_api'] = InputPreprocessor.update_paths(local_payload['comfy_api'], payload_template_map[payload_template_key]['NOISE_SEED']['path'], str(new_seed))
            local_payload['template_inputs']['NOISE_SEED'] = str(new_seed) # Increment the seed by IMG_PER_BATCH for each generation
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"batch {i+1} of {total_batches} - Sending to Runpod - payload['template_inputs'] = {payload['template_inputs']}", level='INFO')
            image_files = await call_runpod(request_id, local_payload, generation_command_args)
            return image_files
        tasks = [fetch_image(i) for i in range(total_batches)]
        images = await asyncio.gather(*tasks)
        image_urls = flatten_list(images) # Flatten the list
        generation_output = json.dumps(image_urls) # Convert the list of image URLs to a JSON string
        print(f"variable images in create_routine: {images}")
        print(f"variable image_urls in create_routine: {image_urls}")
        print(f"variable generation_output: {generation_output}")
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, "Images received. Pushing to Send Queue...", level='INFO')
        generation_output_timetogenerateimagefile=time.time()-generation_output_timespentingenerationqueue-generation_input_timestamp
        await aws_manager.push_send_queue(request_id, username, generation_input_timestamp, payload, generation_command_args, message_data,
                                   generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile)
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Generation data pushed to SendQueue.", level='INFO')
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - create_routine - " + error_message)
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='ERROR')
    finally:
        generationqueue_pop_counter -= 1
        if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT - 1 and MAX_GENERATIONQUEUE_POP_COUNT > 1:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"GenerationQueue reduced to below {MAX_GENERATIONQUEUE_POP_COUNT} (MAX_GENERATIONQUEUE_POP_COUNT).", level='WARNING')
        if generationqueue_pop_counter == 0:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"GenerationQueue reduced to zero.", level='INFO')


async def check_queue_and_create():  # Checks the queue and calls create_routine if there is a tuple
    global generationqueue_pop_counter
    while True:
        try:
            tick_time=set_tick_time(generationqueue_pop_counter)
            if generationqueue_pop_counter < MAX_GENERATIONQUEUE_POP_COUNT: # If the number of requests popped from GenerationQueue is less than the max, pop a request and call create_routine
                aws_manager = await distillery_aws.AWSManager.get_instance()
                result = await aws_manager.pop_generation_queue()
                await asyncio.sleep(tick_time)
                if result is not None:
                    generationqueue_pop_counter += 1
                    if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT:
                        aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"MAX_GENERATIONQUEUE_POP_COUNT ({MAX_GENERATIONQUEUE_POP_COUNT}) reached!", level='WARNING')
                    loop = asyncio.get_event_loop()
                    loop.create_task(create_routine(result))
            else:
                await asyncio.sleep(tick_time)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(INSTANCE_IDENTIFIER + " - check_queue_and_create - " + error_message)
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, error_message, level='ERROR')


# 4. Initialization and shutdown functions
async def main(): # Main function
    aws_manager = await distillery_aws.AWSManager.get_instance()
    loop = asyncio.get_event_loop()
    try:
        queue_task = loop.create_task(check_queue_and_create())
        while True:
            await asyncio.sleep(1)  # sleep for a bit to keep the loop from being too busy
    except asyncio.CancelledError:
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cancelled. Performing cleanup.", level='INFO')
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - main - " + error_message)
        aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, error_message, level='ERROR')
    finally:
        queue_task.cancel()
        try:
            await queue_task
        except asyncio.CancelledError:
            aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Queue task cancelled. Performing cleanup.", level='INFO')
        await aws_manager.close_database_conn()  
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cleanup complete.", level='INFO')

asyncio.run(main())
