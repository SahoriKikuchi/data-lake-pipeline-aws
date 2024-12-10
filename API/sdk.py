#PARA RODAR UTILIZE O COMANDO: python3 ./sdk.py
from flask import Flask, jsonify, request, send_file # python3 -m pip install flask
import boto3
from io import BytesIO
import pandas as pd  # python3 -m pip install pandas
import csv

app = Flask(__name__)

session = boto3.Session(
    aws_access_key_id='******************', #precisa ser preenchido
    aws_secret_access_key='**************************', #precisa ser preenchido
    region_name='sa-east-1'
)
s3_client = session.client('s3')

#OBTEM A LISTA DE BUCKETS
@app.route('/buckets', methods=['GET'])
def list_buckets():
    buckets = s3_client.list_buckets()['Buckets']  
    bucket_names = [bucket['Name'] for bucket in buckets] 
    return jsonify(bucket_names) 

#LISTAR OBJETO DENTRO DO BUCKET
@app.route('/buckets/<bucket_name>/objects', methods=['GET'])
def list_bucket_objects(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=100) 
    contents = [obj['Key'] for obj in response.get('Contents', [])]  
    return jsonify(contents) 

#DOWNLOAD DE UM OBJETO
@app.route('/buckets/<bucket_name>/objects/download', methods=['GET'])
def download_object(bucket_name):
    object_key = request.args.get('object_key')
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key) 
    file_stream = BytesIO(obj['Body'].read())
    return send_file(file_stream, as_attachment=True, download_name=object_key)

#UPLOAD DE UM OBKETO
@app.route('/buckets/<bucket_name>/objects/upload', methods=['POST'])
def upload_object(bucket_name):
    file = request.files['file' ]
    s3_client.upload_fileobj(file, bucket_name, file.filename) 
    return jsonify({"message ": "Upload realizado com sucesso" })

#OBTER METADADOS DE UM OBJETO
@app.route('/buckets/<bucket_name>/objects/metadata', methods=['GET'])
def get_object_metadata(bucket_name):
    object_key = request.args.get('object_key') 
    metadata = s3_client.head_object(Bucket=bucket_name, Key=object_key)['Metadata'] 
    return jsonify(metadata) 

# ATUALIZAR A TAG DE UM OBJETO
@app.route('/buckets/<bucket_name>/objects/tags', methods=['PUT'])
def update_object_tags(bucket_name):
    object_key = request.args.get('object_key') 
    tags = request.json.get('tags', {})  
    tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()] 
    s3_client.put_object_tagging(Bucket=bucket_name, Key=object_key, Tagging={'TagSet': tag_set}) 
    return jsonify({"message": "Tags atualizadas com sucesso"}) 

# DELETA ARQUIVO
@app.route('/buckets/<bucket_name>/objects', methods=['DELETE'])
def delete_object(bucket_name):
    object_key = request.args.get('object_key') 
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)  
    return jsonify({"message": "Objeto deletado com sucesso"})  

# DELETE BUCKET
@app.route('/buckets', methods=['DELETE'])
def delete_bucket():
    bucket_name = request.args.get('bucket_name') 
    response = s3_client.list_objects_v2(Bucket=bucket_name)  
    for obj in response.get('Contents', []):
        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])  
    s3_client.delete_bucket(Bucket=bucket_name)  
    return jsonify({"message": "Bucket deletado com sucesso"})

@app.route('/buckets/<bucket_name>/objects/<path:object_key>', methods=['GET'])
def read_csv(bucket_name, object_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    
    lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(lines)
    data = list(reader)

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True) 
