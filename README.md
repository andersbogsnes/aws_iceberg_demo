# There's An Iceberg In My Lakehouse - What is Apache Iceberg and Why Should You Care?

This is the companion repo to my talk on Apache Iceberg. It contains the code for showing off the various
features of Apache Iceberg, as well as utility code to easily switch between running on AWS or locally in Docker.

## Data source
The demo uses the data from this Kaggle dataset, fetching the expanded dataset directly from the source referenced
in the Kaggle Dataset documentation. It is an event stream from a chinese online retailer, and the total dataset
is around 400M rows. To speed up the demo, we will also create a downsampled dataset to avoid wasting demo time 
uploading all the data
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

## Install the package

### UV (recommended)
```bash
uv sync
```

### pip
Make sure to run inside an activated virtual environment!
```bash
pip install .
```

## Download the data
As part of the python package, there is a CLI named `demo`. 
This will concurrently download the data files and gunzip them. You can control 
concurrency with the `--download-concurrency` and `--extract-concurrency` flag 
```bash
uv run demo download
```

# Running the demo
The code is setup to work with a local Docker Compose setup - if you
d like to switch to using AWS, set the `TUTORIAL_TYPE` env variable to `aws`

If running on AWS, docker compose is not required.

## Running locally

## Startup
Start the docker containers

```bash
docker compose up -d
```

Run the bootstrap command to initialize the local setup
```bash
uv run demo bootstrap
```

Proceed to `tutorial_01.py`