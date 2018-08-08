#!/bin/bash

## geocode test
gcloud dataproc jobs submit pyspark --cluster master geocoding.py