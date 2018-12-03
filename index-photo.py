import json
import boto3
import logging
from botocore.vendored import requests

def lambda_handler(event,context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    rekognition = boto3.client('rekognition')
    
    logger.info('got event{}'.format(event))
    fileName=event["Records"][0]["s3"]["object"]["key"]
    bucket=event["Records"][0]["s3"]["bucket"]["name"]
    time=event["Records"][0]["eventTime"]
    # use rekognition detect labels
    response = rekognition.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':fileName}}, MaxLabels=10, MinConfidence=70)
    logger.info(response)
    # construct the index to store to elastic search
    putlabel = []
    for label in response['Labels']:
        # print (label['Name'] + ' : ' + str(label['Confidence']))
        putlabel.append(label['Name'])
    
    index = {"objectKey": fileName, "bucket": bucket,"createdTimestamp": time, "labels": labels}
    print(index)
    logger.info(index)
    
    # Store to ElasticSearch
    host = "https://vpc-photos-wyjpy5fs4j7w7gpwkudgnpyopm.us-east-1.es.amazonaws.com"
    post_url = host + "/searchphotos/_doc"
    response = requests.post(post_url, json=index)
    response = response.text
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

