#!/bin/bash

dir=/usr/share/mhi-pipeline
py_version=3.6

python$py_version $dir/data_transfer/anonymisation/db_data_scrubber.py $dir/data_transfer/anonymisation/config/run_config.yml


