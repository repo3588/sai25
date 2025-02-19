import sys
import pandas as pd
import boto3
from io import BytesIO
from awsglue.utils import getResolvedOptions

# Get input arguments from AWS Glue
args = getResolvedOptions(sys.argv, ['S3_INPUT_BUCKET', 'S3_INPUT_KEY', 'S3_OUTPUT_BUCKET', 'S3_OUTPUT_KEY'])

s3_input_bucket = args['S3_INPUT_BUCKET']
s3_input_key = args['S3_INPUT_KEY']
s3_output_bucket = args['S3_OUTPUT_BUCKET']
s3_output_key = args['S3_OUTPUT_KEY']

def merge_excel_sheets(s3_bucket, s3_key, output_s3_bucket, output_s3_key):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Read Excel file from S3
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    excel_data = response['Body'].read()
    
    # Read all sheets into a dictionary of DataFrames
    sheets_dict = pd.read_excel(BytesIO(excel_data), sheet_name=None)
    
    # List to hold individual DataFrames
    merged_data = []
    
    for sheet_name, df in sheets_dict.items():
        # Extract prefix before '_'
        source_name = sheet_name.split('_')[0]
        
        # Add column with source sheet name prefix
        df['Source'] = source_name
        
        # Append to list
        merged_data.append(df)
    
    # Combine all DataFrames into one
    final_df = pd.concat(merged_data, ignore_index=True)
    
    # Save final dataframe to S3
    output_buffer = BytesIO()
    final_df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)
    s3.put_object(Bucket=output_s3_bucket, Key=output_s3_key, Body=output_buffer)
    
    return final_df

# Execute function
final_dataframe = merge_excel_sheets(s3_input_bucket, s3_input_key, s3_output_bucket, s3_output_key)
print(final_dataframe.head())
