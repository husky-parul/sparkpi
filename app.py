import time
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession

import json
import requests 

if __name__ == "__main__":

    dataverse_server = 'http://128.31.24.163:8080' # no trailing slash
    api_key = 'f86fa7e2-29be-4c00-b6d2-b83acb6e5f4b'
    dataverse_id = "root" #database id of the dataverse 
    url_dataverse_id = '%s/api/dataverses/%s/datasets/?key=%s' % (dataverse_server, dataverse_id, api_key)

    # # --------------------------------------------------
    # # Using a "jsonData" parameter, add description for dataset
    # # --------------------------------------------------
    with open('dataset.json') as dataset_json_file:
	    file = dataset_json_file.read()
    data_load = json.loads(file)
    data = json.dumps(data_load)

  

    # # -------------------
    # # Make the request
    # # -------------------
    print('-' * 40)
    print('making request: %s' % url_dataverse_id)
    r = requests.post(url_dataverse_id, data=data)

    # # -------------------
    # # Print the response
    # # -------------------
    print('-' * 40)
    print(r.status_code)
    json_str=r.json()
    print 'This is json_str \n',json_str


    # --------------------------------------------------
    # Get id and persistentId of created dataset
    # --------------------------------------------------
    dataset_id = json_str['data']['id'] # database id of the dataset
    
    # --------------------------------------------------
    # Prepare "file"
    # --------------------------------------------------
    file_content = 'content: %s' % datetime.now()
    files = {'file': ('sample_file.txt', file_content)}

    # --------------------------------------------------
    # Using a "jsonData" parameter, add optional description + file tags
    # --------------------------------------------------
    params = dict(description='Blue skies!',categories=['Lily', 'Rosemary', 'Jack of Hearts'])
    params_as_json_string = json.dumps(params)
    payload = dict(jsonData=params_as_json_string)

    # --------------------------------------------------
    # Add file using the Dataset's id
    # --------------------------------------------------
    # curl version
    # POST http://$SERVER/api/datasets/$id/add?key=$apiKey
    url_dataset_id = '%s/api/datasets/%s/add?key=%s' % (dataverse_server, dataset_id, api_key)


    # -------------------
    # Make the request
    # -------------------
    print('-' * 40)
    print('making request: %s' % url_dataset_id)
    r = requests.post(url_dataset_id, data=payload, files=files)

    # -------------------
    # Print the response
    # -------------------
    print('-' * 40)
    print(r.json())
    print(r.status_code)