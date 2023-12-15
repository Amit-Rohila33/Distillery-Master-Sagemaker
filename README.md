## UnifiedDistilleryWorkflow.py

### Overview

The `UnifiedDistilleryWorkflow` is a Python script designed to manage and distribute image generation tasks to different services, specifically tailored for Runpod and SageMaker. It leverages asyncio for asynchronous processing and integrates with various AWS services for image generation and storage.

### Prerequisites

- Python 3.7 or higher
- Required Python packages (install using `pip install <package>`):
  - aiohttp
  - boto3
  - pillow
  - runpod (ensure it's installed and configured for Runpod tasks)
  
### Configuration

The script uses a configuration dictionary (`CONFIG`) to set various parameters. Ensure that the following configuration variables are appropriately set in the script:

- `APP_NAME`: Your application name.
- `SAGEMAKER_ENDPOINT_NAME`: Name of your SageMaker endpoint.
- `S3_BUCKET_NAME`: Name of your S3 bucket for storing generated images.
- `S3_OUTPUT_PATH`: Path within the S3 bucket for storing generated images.
- `SECONDS_PER_TICK`: Time interval between each iteration.

### Usage

#### For Runpod

1. Set the `RUNPOD_API_KEY` environment variable.
2. Instantiate a `DistilleryMaster` object with `service_type='runpod'`.
3. Run the `main` method to start processing Runpod tasks.

```python
runpod_master = DistilleryMaster(service_type='runpod')
asyncio.run(runpod_master.main())
```

### For SageMaker

1. Instantiate a DistilleryMaster object with service_type='sagemaker'.
2. Run the main method to start processing SageMaker tasks.

```python
sagemaker_master = DistilleryMaster(service_type='sagemaker')
asyncio.run(sagemaker_master.main())
```


## distillery_master_sagemaker.py

### Overview

This Python script is designed for asynchronous image generation using SageMaker as the backend service. It continually checks a generation queue for tasks, processes them asynchronously, and uploads the generated images to an S3 bucket. The script uses the `asyncio` library for concurrent operations and integrates with AWS services, including SageMaker and S3.

### Prerequisites

- Python 3.7 or higher
- Required Python packages (install using `pip install <package>`):
  - aiohttp
  - boto3
  - pillow

### Configuration

The script reads configuration parameters from a JSON file (`config/config.json`). Ensure this file is present and correctly configured. Key parameters include:

- `APP_NAME`: Application name.
- `SECONDS_PER_TICK`: Time interval between iterations.
- `SAGEMAKER_ENDPOINT_NAME`: SageMaker endpoint name.
- `S3_BUCKET_NAME`: Name of the S3 bucket for storing generated images.
- `S3_OUTPUT_PATH`: Path within the S3 bucket for storing images.

### Usage

1. Ensure AWS credentials are configured appropriately for interaction with AWS services.
2. Run the script using the following command:

```bash
python script_name.py
```

### distillery_master_runpod.py

File that you provided to me, `Felipe`
