import boto3
import json
from io import BytesIO
import base64
import csv

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    path = event.get('resource', '')
    http_method = event.get('httpMethod', '')

    try:
        if path == "/buckets" and http_method == "GET":
            return list_buckets(s3_client)
        elif path == "/buckets/{bucket_name}/objects" and http_method == "GET":
            bucket_name = event['pathParameters'].get('bucket_name', None)
            if not bucket_name:
                raise ValueError("bucket_name não achou")
            print("bucket_name:", bucket_name)  
            return list_bucket_objects(s3_client, bucket_name)
        elif path == "/buckets/{bucket_name}/objects/download" and http_method == "GET":
            bucket_name = event['pathParameters'].get('bucket_name', None)
            if not bucket_name:
                raise ValueError("bucket_name n")
            print("bucket_name:", bucket_name) 
            object_key = event['queryStringParameters'].get('object_key', None)
            if not object_key:
                raise ValueError("object_key não achou")
            return download_object(s3_client, bucket_name, object_key)
        elif path == "/buckets/{bucket_name}/objects/read-csv" and http_method == "GET":
            bucket_name = event['pathParameters'].get('bucket_name', None)
            if not bucket_name:
                raise ValueError("bucket_name não achou")
            print("bucket_name:", bucket_name) 
            object_key = event['queryStringParameters'].get('object_key', None)
            if not object_key:
                raise ValueError("object_key não achou")
            return read_csv(s3_client, bucket_name, object_key)
        elif path == "/buckets/{bucket_name}/objects/upload" and http_method == "POST":
            bucket_name = event['pathParameters'].get('bucket_name', None)
            if not bucket_name:
                raise ValueError("bucket_name não achou")
            print("bucket_name:", bucket_name)  
            body = event.get('body', None)
            if not body:
                raise ValueError("body não achou")
            return upload_object(s3_client, bucket_name, body)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': ' invalido ',
                    'received_path': path,
                    'received_method': http_method,
                    'received_bucket_name': bucket_name if 'bucket_name' in locals() else 'bucket_name não definido',
                    'expected_paths': [
                        "/buckets",
                        "/buckets/{bucket_name}/objects",
                        "/buckets/{bucket_name}/objects/download",
                        "/buckets/{bucket_name}/objects/read-csv",
                        "/buckets/{bucket_name}/objects/upload"
                    ]
                })
            }

    except Exception as e:
        print("Erro ocorrido:", str(e))
        if 'bucket_name' in locals() and bucket_name:
            print("bucket_name:", bucket_name) 
        else:
            print("bucket_name não foi definido.")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Erro interno do servidor: {str(e)}',
                'bucket_name': bucket_name,
                'stackTrace': str(e)
            })
        }

def list_buckets(s3_client):
    buckets = s3_client.list_buckets()['Buckets']
    bucket_names = [bucket['Name'] for bucket in buckets]
    return {
        'statusCode': 200,
        'body': json.dumps(bucket_names)
    }

def list_bucket_objects(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=100)
    contents = [obj['Key'] for obj in response.get('Contents', [])]
    return {
        'statusCode': 200,
        'body': json.dumps(contents)
    }

def download_object(s3_client, bucket_name, object_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_stream = BytesIO(obj['Body'].read())
    file_stream.seek(0)
    file_data = base64.b64encode(file_stream.read()).decode('utf-8')
    return {
        'statusCode': 200,
        'body': file_data,
        'isBase64Encoded': True,
        'headers': {
            'Content-Disposition': f'attachment; filename={object_key}'
        }
    }

def upload_object(s3_client, bucket_name, body):
    file_data = base64.b64decode(body)
    file_name = 'uploaded_file' 
    s3_client.upload_fileobj(BytesIO(file_data), bucket_name, file_name)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Upload successful'})
    }

def read_csv(s3_client, bucket_name, object_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    
    lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(lines)
    data = list(reader)  

    return {
        'statusCode': 200,
        'body': json.dumps(data),
        'headers': {
            'Content-Type': 'application/json'
        }
    }
