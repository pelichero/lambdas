import json
import logging
import time
import boto3
import os
from botocore.exceptions import ClientError

def handler(event, context):
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')
    
    recordList = []
    try:
        for record in event['Records']:
            if record['eventName'] == 'REMOVE':
                recordList.append(record)
        handle_remove(recordList)
    except Exception as e:
        logging.error(e)
        return "Error"

def handle_remove(recordList):
    logging.info("Handling REMOVE Event")
    firehose_client = boto3.client('firehose')
    firehose_name = os.environ['firehose_name']
    bucket_arn = os.environ['bucket_arn']
    iam_role_name = os.environ['iam_role_name']
    batch_size = int(os.environ['batch_size'])
    
    for record in recordList:	 
        logging.info ('Row removed with id=' + record['id']['S'])
    
    result=None
    batch=[{'Data': json.dumps(recordList)}]
    try:
        result=firehose_client.put_record_batch(DeliveryStreamName=firehose_name,Records=batch)
    except ClientError as e :
        logging.error(e)
        exit(1)
    if result :
        num_failures = result['FailedPutCount']
        if num_failures:
            logging.info('Resending {num_failures} failed records')
            rec_index = 0
            for record in result['RequestResponses']:
                if 'ErrorCode' in record:
                    firehose_client.put_record(DeliveryStreamName=firehose_name,Record=batch[rec_index])
                    num_failures -= 1
                    if not num_failures:
                        break
                rec_index += 1
    logging.info('Data sent to Firehose stream')