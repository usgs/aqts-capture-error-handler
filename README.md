# Aquarius Time Series (AQTS) Capture Error Handler
[![Build Status](https://travis-ci.org/usgs/aqts-capture-error-handler.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-error-handler)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-error-handler/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-error-handler)


AWS Lambda function designed to persist inputs and exceptions that occur
during AWS Step Function executions.

## Unit Testing
Make sure you have `python`, `pip`, and `venv` installed, navigate to the project root directory and run the following commands:
```shell script
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
python -m unittest -v
```
