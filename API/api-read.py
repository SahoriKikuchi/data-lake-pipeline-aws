import boto3
import json
import csv

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = event.get('pathParameters', {}).get('bucket_name')
    object_key = event.get('queryStringParameters', {}).get('object_key')
    method = event.get('httpMethod', 'method não definido')
    path = event.get('resource', 'path não definido')

    if method != 'GET' or not path.startswith('/buckets'):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Erro na solicitacao",
                "details": {
                    "erro_detectado": "HTTP inválido",
                    "path_recebido": path,
                    "method_recebido": method,
                    "bucket_name_recebido": bucket_name or "não definido",
                    "object_key_recebido": object_key or "não definido"
                },
                "solucao": "Verifique se o caminho, método HTTP, nome do bucket e object_key estão corretos.",
                "expected_paths": [
                    "/buckets",
                    "/buckets/{bucket_name}/objects",
                    "/buckets/{bucket_name}/objects/download",
                    "/buckets/{bucket_name}/objects/read-csv",
                    "/buckets/{bucket_name}/objects/upload"
                ]
            })
        }
    
    if path.startswith("/buckets/") and "/objects/read-csv" in path and method == "GET":
        return read_csv(s3_client, bucket_name, object_key)

    else:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'não encontrado',
                'received_path': path,
                'received_method': method,
                'received_bucket_name': bucket_name or "não definido",
                'received_object_key': object_key or "não definido",
                'expected_paths': [
                    "/buckets",
                    "/buckets/{bucket_name}/objects",
                    "/buckets/{bucket_name}/objects/download",
                    "/buckets/{bucket_name}/objects/read-csv",
                    "/buckets/{bucket_name}/objects/upload"
                ]
            })
        }

def read_csv(s3_client, bucket_name, object_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(lines)
        data = list(reader)
        return {
            'statusCode': 200,
            'body': json.dumps(data)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Erro interno do servidor: {str(e)}',
                'bucket_name': bucket_name,
                'object_key': object_key
            })
        }
