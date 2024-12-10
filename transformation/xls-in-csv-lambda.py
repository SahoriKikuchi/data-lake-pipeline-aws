import json
import boto3
import xlrd
import io
import csv
import os

def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    
    if event:
        s3_event = event["Records"][0]
        bucket_name = s3_event["s3"]["bucket"]["name"]
        file_name = s3_event["s3"]["object"]["key"]
        
        specific_cases = [
            "CEPEA_anual_cafeArabica-1994-2023.xls",
            "CEPEA_anual_cafeRobusta-1994-2023.xls",
            "CEPEA_mensal_cafeArabica-1994-2023.xls",
            "CEPEA_mensal_cafeRobusta-1994-2023.xls"
        ]
        
        if file_name in specific_cases:
            skip_rows = 2
        else:
            skip_rows = 0
        
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        excel_data = response["Body"]
        
        book = xlrd.open_workbook(file_contents=excel_data.read(), encoding_override="utf-16-le", ignore_workbook_corruption=True)
        sheet = book.sheet_by_index(0) 
        
        file_base_name, file_extension = os.path.splitext(file_name)
        csv_file_path = f"/tmp/{file_base_name}.csv"
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            for rownum in range(skip_rows, sheet.nrows):
                csv_writer.writerow([str(cell_value).replace(',', '.') for cell_value in sheet.row_values(rownum)])
        
        folder_name = f"{file_base_name}/"
        s3_key = f"{folder_name}{file_base_name}.csv"
        
        s3_resource.Bucket("processed-data-harvestlake").upload_file(csv_file_path, s3_key)
        
        os.remove(csv_file_path)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Conversão realizada com sucesso :) ')
    }
