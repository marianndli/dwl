import json
import boto3
import requests
import math
#import datetime

def lambda_handler(event, context):
    
    try:
        # Fetch JSON data from the API
        
        # API URL for the data
        api_url = "https://data.sbb.ch/api/explore/v2.1/catalog/datasets/ist-daten-sbb/records?limit=100&offset=0&refine=faellt_aus_tf%3A%22true%22"
        
        # Fetch JSON data from the API
        response = requests.get(api_url)
        data = response.json()
        
        total_count_interruptions=data["total_count"]
        
        
        
        if total_count_interruptions <= 100:
            Number_of_necessary_iterations = 0
        else:
            Number_of_necessary_iterations= total_count_interruptions/100
        Number_of_necessary_iterations=math.floor(Number_of_necessary_iterations)
        
        
        url_firstpart= "https://data.sbb.ch/api/explore/v2.1/catalog/datasets/ist-daten-sbb/records?limit=100&offset="
        url_lastpart="&refine=faellt_aus_tf%3A%22true%22"
        
        if Number_of_necessary_iterations >0:
            
            act_offset = 100
            for i in range(1, Number_of_necessary_iterations + 1):
                act_url=url_firstpart+str(act_offset)+url_lastpart
                response_new = requests.get(act_url)
                data_new = response_new.json()
                data["results"].extend(data_new["results"])
                act_offset += 100
        
        
        betriebstag = data["results"][0]["betriebstag"]
        #current_time = datetime.datetime.now()
        #formatted_time = current_time.strftime("%H:%M:%S")
                

        # Initialize S3 client
        s3 = boto3.client('s3')

        # Save JSON data to S3 bucket
        bucket_name = 'datalakepartition'
        key = betriebstag+'-interruptions.json'
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))

        return {
            'statusCode': 200,
            'body': json.dumps('JSON data saved to S3')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')}
