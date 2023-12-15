import os
import asyncio
import json
import signal
import boto3
import base64
import io
from PIL import Image
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

# Extracting configuration values
APP_NAME = CONFIG['APP_NAME']
SECONDS_PER_TICK = CONFIG['SECONDS_PER_TICK']
MAX_GENERATIONQUEUE_POP_COUNT = int(CONFIG['MAX_GENERATIONQUEUE_POP_COUNT'])
SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT = int(CONFIG['SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT'])
SAGEMAKER_ENDPOINT_NAME = CONFIG['SAGEMAKER_ENDPOINT_NAME']
S3_BUCKET_NAME = CONFIG['S3_BUCKET_NAME']
S3_OUTPUT_PATH = CONFIG['S3_OUTPUT_PATH']

######## End of inputs; no other inputs are made below this line
INSTANCE_IDENTIFIER = APP_NAME + '-' + str(uuid.uuid4())
MAX_RUNPOD_ATTEMPTS = 3
generationqueue_pop_counter = 0

sagemaker_client = boto3.client('sagemaker')
s3_client = boto3.client('s3')

# 1. Support functions

# Function to calculate tick time based on the generation queue pop counter
def set_tick_time(generationqueue_pop_counter):
    tick_time = SECONDS_PER_TICK * (1 + SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT * generationqueue_pop_counter)
    return tick_time

# Function to flatten a nested list
def flatten_list(nested_list):
    flat_list = []
    for item in nested_list:
        if isinstance(item, list):
            flat_list.extend(flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list

# 2. Image generation functions

# Function to call the SageMaker endpoint for image generation
async def call_sagemaker(request_id, payload, command_args):
    for attempt in range(MAX_RUNPOD_ATTEMPTS):
        try:
            # Assuming payload is a JSON string
            response = sagemaker_client.invoke_endpoint(
                EndpointName=SAGEMAKER_ENDPOINT_NAME,
                Body=json.dumps(payload),
                ContentType='application/json',
            )
            output = response['Body'].read().decode('utf-8')
            return output
        except Exception as e:
            # Handle exceptions and print error messages
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no} (attempt {attempt+1}): {str(e)}'
            print(INSTANCE_IDENTIFIER + " - call_sagemaker - " + error_message)
            if attempt < 2:
                print(error_message)
            else:
                print(error_message)

# 3. Creation functions that will be running in loop

# Async function to handle image generation routine
async def create_routine(tuple):
    global generationqueue_pop_counter
    try:
        # Get AWSManager instance
        aws_manager = await distillery_aws.AWSManager.get_instance()
        # Extract parameters from the input tuple
        request_id = tuple[0]
        username = tuple[1]
        generation_input_timestamp = tuple[2]
        payload = tuple[3]
        generation_command_args = tuple[4]
        message_data = tuple[5]
        generation_other_data = tuple[6]
        generation_output_timespentingenerationqueue = tuple[7]
        total_batches = generation_command_args['TOTAL_BATCHES']
        image_urls = []
        starting_seed = int(payload['template_inputs']['NOISE_SEED'])
        payload_template_key = payload['template_inputs']['PAYLOAD_TEMPLATE_KEY']
        payload_template_map = json.load(open(CONFIG['COMFY_TEMPLATE_MAP']))
        
        # Async function to fetch images
        async def fetch_image(i):
            local_payload = copy.deepcopy(payload)
            new_seed = starting_seed + i * int(generation_command_args['IMG_PER_BATCH'])
            local_payload['comfy_api'] = InputPreprocessor.update_paths(local_payload['comfy_api'], payload_template_map[payload_template_key]['NOISE_SEED']['path'], str(new_seed))
            local_payload['template_inputs']['NOISE_SEED'] = str(new_seed)
            image_files = await call_sagemaker(request_id, local_payload, generation_command_args)
            return image_files
        
        # Create a list of tasks to fetch images asynchronously
        tasks = [fetch_image(i) for i in range(total_batches)]
        # Gather results from the tasks
        images = await asyncio.gather(*tasks)
        # Flatten the list of images
        image_urls = flatten_list(images)
        # Convert the image URLs to a JSON string
        generation_output = json.dumps(image_urls)
        # Print debugging information
        print(f"variable images in create_routine: {images}")
        print(f"variable image_urls in create_routine: {image_urls}")
        print(f"variable generation_output: {generation_output}")
        # Log the event
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, "Images received. Pushing to Send Queue...", level='INFO')
        generation_output_timetogenerateimagefile = time.time() - generation_output_timespentingenerationqueue - generation_input_timestamp

        # Upload the image files to S3
        s3_output_folder = f"{S3_OUTPUT_PATH}/{request_id}"
        for i, image_data in enumerate(images):
            image_filename = f"image_{i+1}.png"
            image_bytes = base64.b64decode(image_data)
            s3_key = f"{s3_output_folder}/{image_filename}"
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=image_bytes)

        s3_urls = [f"s3://{S3_BUCKET_NAME}/{s3_output_folder}/{image_filename}" for i in range(total_batches)]

        # Push the S3 URLs to SendQueue
        await aws_manager.push_send_queue(request_id, username, generation_input_timestamp, payload, generation_command_args, message_data,
                                          generation_other_data, s3_urls, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile)
        # Log the event
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Generation data pushed to SendQueue.", level='INFO')
    except Exception as e:
        # Handle exceptions and log error messages
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - create_routine - " + error_message)
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='ERROR')
    finally:
        # Decrement the generation queue pop counter
        generationqueue_pop_counter -= 1
        # Check if the generation queue pop counter has reached a specific threshold
        if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT - 1 and MAX_GENERATIONQUEUE_POP_COUNT > 1:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"GenerationQueue reduced to below {MAX_GENERATIONQUEUE_POP_COUNT} (MAX_GENERATIONQUEUE_POP_COUNT).", level='WARNING')
        if generationqueue_pop_counter == 0:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"GenerationQueue reduced to zero.", level='INFO')

# Async function to check the queue and initiate image creation
async def check_queue_and_create():
    global generationqueue_pop_counter
    while True:
        try:
            # Calculate the tick time based on the generation queue pop counter
            tick_time = set_tick_time(generationqueue_pop_counter)
            # Check if the generation queue pop counter is below the maximum count
            if generationqueue_pop_counter < MAX_GENERATIONQUEUE_POP_COUNT:
                # Get AWSManager instance
                aws_manager = await distillery_aws.AWSManager.get_instance()
                # Pop an item from the generation queue
                result = await aws_manager.pop_generation_queue()
                # Wait for the calculated tick time
                await asyncio.sleep(tick_time)
                # Check if an item is retrieved from the generation queue
                if result is not None:
                    # Increment the generation queue pop counter
                    generationqueue_pop_counter += 1
                    # Check if the maximum generation queue pop count is reached
                    if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT:
                        aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"MAX_GENERATIONQUEUE_POP_COUNT ({MAX_GENERATIONQUEUE_POP_COUNT}) reached!", level='WARNING')
                    # Create a new task for image generation routine
                    loop = asyncio.get_event_loop()
                    loop.create_task(create_routine(result))
            else:
                # Wait for the calculated tick time
                await asyncio.sleep(tick_time)
        except Exception as e:
            # Handle exceptions and log error messages
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(INSTANCE_IDENTIFIER + " - check_queue_and_create - " + error_message)
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, error_message, level='ERROR')

# Main async function
async def main():
    aws_manager = await distillery_aws.AWSManager.get_instance()
    loop = asyncio.get_event_loop()
    try:
        # Create a task for the check_queue_and_create function
        queue_task = loop.create_task(check_queue_and_create())
        # Run an infinite loop with a short sleep
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        # Handle cancellation of the main function
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cancelled. Performing cleanup.", level='INFO')
    except Exception as e:
        # Handle exceptions in the main function and log error messages
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - main - " + error_message)
        aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, error_message, level='ERROR')
    finally:
        # Cancel the queue task
        queue_task.cancel()
        try:
            # Wait for the queue task to finish
            await queue_task
        except asyncio.CancelledError:
            aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Queue task cancelled. Performing cleanup.", level='INFO')
        # Close the database connection
        await aws_manager.close_database_conn()
        # Log the cleanup completion
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cleanup complete.", level='INFO')

# Run the main function using asyncio.run()
asyncio.run(main())
