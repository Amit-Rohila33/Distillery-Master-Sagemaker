import asyncio
import json
import os
import uuid
import sys
import aiohttp
from distillery_aws import AWSManager
from distillery_commands import InputPreprocessor
import runpod
from runpod import AsyncioEndpoint, AsyncioJob
import boto3
from PIL import Image
import base64
import io
import time
import random

CONFIG = {
    'APP_NAME': 'YourAppName',
    'MAX_GENERATIONQUEUE_POP_COUNT': '10',
    'SAGEMAKER_ENDPOINT_NAME': 'YourSageMakerEndpointName',
    'S3_BUCKET_NAME': 'YourS3BucketName',
    'S3_OUTPUT_PATH': 'YourS3OutputPath',
    'SECONDS_PER_TICK': 5,
    'SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT': 2,
}


def flatten_list(nested_list):
    flattened_list = [item for sublist in nested_list for item in sublist]
    return flattened_list


class DistilleryMaster:
    def __init__(self, service_type):
        self.service_type = service_type
        self.generationqueue_pop_counter = 0
        self.instance_identifier = f"{CONFIG['APP_NAME']}-{str(uuid.uuid4())}"
        self.max_generationqueue_pop_count = int(CONFIG['MAX_GENERATIONQUEUE_POP_COUNT'])
        self.sagemaker_endpoint_name = CONFIG['SAGEMAKER_ENDPOINT_NAME']
        self.s3_bucket_name = CONFIG['S3_BUCKET_NAME']
        self.s3_output_path = CONFIG['S3_OUTPUT_PATH']
        self.seconds_per_tick = CONFIG['SECONDS_PER_TICK']
        self.seconds_per_tick_multiplier_per_pop_count = CONFIG['SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT']
        self.max_runpod_attempts = 3

        if service_type == 'runpod':
            runpod.api_key = os.getenv('RUNPOD_API_KEY')
        elif service_type == 'sagemaker':
            self.sagemaker_client = boto3.client('sagemaker')
            self.s3_client = boto3.client('s3')

    async def call_service(self, request_id, payload, command_args):
        if self.service_type == 'runpod':
            return await self.call_runpod(request_id, payload, command_args)
        elif self.service_type == 'sagemaker':
            return await self.call_sagemaker(request_id, payload, command_args)

    async def call_runpod(self, request_id, payload, command_args):
        for attempt in range(self.max_runpod_attempts):
            try:
                aws_manager = await AWSManager.get_instance()
                if isinstance(payload, str):
                    payload = json.loads(payload)
                async with aiohttp.ClientSession() as session:
                    endpoint = runpod.AsyncioEndpoint(command_args['ENDPOINT_ID'], session)
                    job = await endpoint.run(payload)
                    status = await job.status()
                    aws_manager.print_log(request_id, self.instance_identifier, f"Runpod Status for request_id {request_id}: {status}", level="INFO")
                    output = await job.output()
                    aws_manager.print_log(request_id, self.instance_identifier, f"Runpod Output for request_id {request_id}: {output}", level="INFO")
                print("--------------------------------------------")
                aws_manager.print_log(request_id, self.instance_identifier, f"Runpod called successfully. Output: {output}", level='INFO')
                return output
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                line_no = exc_traceback.tb_lineno
                error_message = f'Unhandled error at line {line_no} (attempt {attempt+1}): {str(e)}'
                print(self.instance_identifier + " - call_runpod - " + error_message)
                if attempt < 2:
                    aws_manager.print_log(request_id, self.instance_identifier, error_message, level='WARNING')
                else:
                    aws_manager.print_log(request_id, self.instance_identifier, error_message, level='ERROR')

    async def call_sagemaker(self, request_id, payload, command_args):
        for attempt in range(self.max_runpod_attempts):
            try:
                response = self.sagemaker_client.invoke_endpoint(
                    EndpointName=self.sagemaker_endpoint_name,
                    Body=json.dumps(payload),
                    ContentType='application/json',
                )
                output = response['Body'].read().decode('utf-8')
                return output
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                line_no = exc_traceback.tb_lineno
                error_message = f'Unhandled error at line {line_no} (attempt {attempt+1}): {str(e)}'
                print(self.instance_identifier + " - call_sagemaker - " + error_message)
                if attempt < 2:
                    print(error_message)
                else:
                    print(error_message)

    async def create_routine(self, tuple):
        try:
            aws_manager = await AWSManager.get_instance()
            request_id, username, generation_input_timestamp, payload, generation_command_args, message_data, generation_other_data, generation_output_timespentingenerationqueue = tuple
            total_batches = generation_command_args['TOTAL_BATCHES']
            image_urls = []

            async def fetch_image(i):
                local_payload = copy.deepcopy(payload)
                new_seed = int(payload['template_inputs']['NOISE_SEED']) + i * int(generation_command_args['IMG_PER_BATCH'])
                local_payload['comfy_api'] = InputPreprocessor.update_paths(local_payload['comfy_api'],
                                                                             payload_template_map[payload_template_key]['NOISE_SEED']['path'], str(new_seed))
                local_payload['template_inputs']['NOISE_SEED'] = str(new_seed)
                image_files = await self.call_service(request_id, local_payload, generation_command_args)
                return image_files

            tasks = [fetch_image(i) for i in range(total_batches)]
            images = await asyncio.gather(*tasks)
            image_urls = flatten_list(images)
            generation_output = json.dumps(image_urls)
            print(f"variable images in create_routine: {images}")
            print(f"variable image_urls in create_routine: {image_urls}")
            print(f"variable generation_output: {generation_output}")
            aws_manager.print_log(request_id, self.instance_identifier, "Images received. Pushing to Send Queue...", level='INFO')
            generation_output_timetogenerateimagefile = time.time() - generation_output_timespentingenerationqueue - generation_input_timestamp

            if self.service_type == 'sagemaker':
                s3_output_folder = f"{self.s3_output_path}/{request_id}"
                for i, image_data in enumerate(images):
                    image_filename = f"image_{i+1}.png"
                    image_bytes = base64.b64decode(image_data)
                    s3_key = f"{s3_output_folder}/{image_filename}"
                    self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=s3_key, Body=image_bytes)

                s3_urls = [f"s3://{self.s3_bucket_name}/{s3_output_folder}/{image_filename}" for i in range(total_batches)]

                await aws_manager.push_send_queue(request_id, username, generation_input_timestamp, payload, generation_command_args, message_data,
                                                  generation_other_data, s3_urls, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile)
            else:
                await aws_manager.push_send_queue(request_id, username, generation_input_timestamp, payload, generation_command_args, message_data,
                                                  generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile)

            aws_manager.print_log(request_id, self.instance_identifier, f"Generation data pushed to SendQueue.", level='INFO')
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(self.instance_identifier + " - create_routine - " + error_message)
            aws_manager.print_log(request_id, self.instance_identifier, error_message, level='ERROR')

    async def check_queue_and_create(self):
        while True:
            try:
                tick_time = self.set_tick_time(self.generationqueue_pop_counter)
                if self.generationqueue_pop_counter < self.max_generationqueue_pop_count:
                    aws_manager = await AWSManager.get_instance()
                    result = await aws_manager.pop_generation_queue()
                    await asyncio.sleep(tick_time)
                    if result is not None:
                        self.generationqueue_pop_counter += 1
                        if self.generationqueue_pop_counter == self.max_generationqueue_pop_count:
                            aws_manager.print_log("N/A", self.instance_identifier,
                                                  f"MAX_GENERATIONQUEUE_POP_COUNT ({self.max_generationqueue_pop_count}) reached!", level='WARNING')
                        loop = asyncio.get_event_loop()
                        loop.create_task(self.create_routine(result))
                else:
                    await asyncio.sleep(tick_time)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                line_no = exc_traceback.tb_lineno
                error_message = f'Unhandled error at line {line_no}: {str(e)}'
                print(self.instance_identifier + " - check_queue_and_create - " + error_message)
                aws_manager.print_log("N/A", self.instance_identifier, error_message, level='ERROR')

    async def main(self):
        aws_manager = await AWSManager.get_instance()
        loop = asyncio.get_event_loop()
        try:
            queue_task = loop.create_task(self.check_queue_and_create())
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            aws_manager.print_log('N/A', self.instance_identifier, "Main function cancelled. Performing cleanup.", level='INFO')
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(self.instance_identifier + " - main - " + error_message)
            aws_manager.print_log("N/A", self.instance_identifier, error_message, level='ERROR')
        finally:
            queue_task.cancel()
            try:
                await queue_task
            except asyncio.CancelledError:
                aws_manager.print_log('N/A', self.instance_identifier, "Queue task cancelled. Performing cleanup.", level='INFO')
            await aws_manager.close_database_conn()
            aws_manager.print_log('N/A', self.instance_identifier, "Main function cleanup complete.", level='INFO')

    def set_tick_time(self, generationqueue_pop_counter):
        tick_time = self.seconds_per_tick * (1 + self.seconds_per_tick_multiplier_per_pop_count * generationqueue_pop_counter)
        return tick_time

# For Runpod
runpod_master = DistilleryMaster(service_type='runpod')
asyncio.run(runpod_master.main())

# For SageMaker
sagemaker_master = DistilleryMaster(service_type='sagemaker')
asyncio.run(sagemaker_master.main())
