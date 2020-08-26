from secrets import secrets as sec
import snowflake.connector as sf
import boto3
from botocore.exceptions import NoCredentialsError
import os
import sys
from time import process_time
import pandas as pd


df = your_df #pandas dataframe
schema = '' #snowflake schema of your table
table = '' #snowflake table name
database = '' #snowflake database of your table
warehouse = '' #snowflake warehouse of your table
s3_bucket = '' #s3 bucket where the csv file should be placed


# For larger Dataframes one should not use Snowflake's own Pandas Dataframe to Snowflake function
# because of performance issues.
# Instead it is much faster to create a csv file from Dataframe, write to S3 and then upload to Snowflake.

print('------------------------------------')
print('Start: Pandas Dataframe to Snowflake')
print('------------------------------------')


def upload_to_aws(local_file, s3_file, bucket):
    s3 = boto3.client('s3',
                      aws_access_key_id=sec.aws_key_id,
                      aws_secret_access_key=sec.aws_secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f'Success: S3 Upload Successful ({bucket}/{s3_file})')
        return True
    except FileNotFoundError:
        print("Failure: The file for S3 upload was not found")
        return False
    except NoCredentialsError:
        print("Failure: S3 Credentials not available")
        return False


def write_to_s3(df, schema, table, s3_bucket):
    df.columns = map(str.upper, df.columns)
    df.to_csv(f'{schema}.{table}.csv.gz', index=False, compression='gzip', sep=',')
    upload_to_aws(f'{schema}.{table}.csv.gz', f'{schema}.{table}.csv.gz', s3_bucket)
    os.remove(f'{schema}.{table}.csv.gz')


def write_to_sf(df, schema, table, database, warehouse, s3_bucket):
    try:
        write_to_s3(df, schema, table, s3_bucket)

        snowflake_connection = sf.connect(account=sec.account, user=sec.username,
                                          password=sec.password, database=database,
                                          warehouse=warehouse, schema=schema, autocommit=True)

        cursor = snowflake_connection.cursor()


        column_list = df.columns.values.tolist()
        column_list_copy = ", ".join(str(x) for x in column_list)
        number_list_copy = ", ".join(('$' + str((i+1))) for i in range(len(df.columns)))

        cursor.execute(f"""copy into {schema}.{table}
                            from s3://{s3_bucket}/{schema}.{table}.csv.gz credentials=(aws_key_id='{sec.aws_key_id}' aws_secret_key='{sec.aws_secret_key}')
                                
                          file_format=(TYPE=CSV 
                                        SKIP_HEADER=1
                                        FIELD_DELIMITER=','
                                        EMPTY_FIELD_AS_NULL=TRUE)
                          ;""")

        print(f'Success: Snowflake Upload Successful ({schema}.{table})')

    except Exception as e:
        print(f'Failure: {e}')
        sys.exit()


if __name__ == '__main__':
    time_start = process_time()

    try:
        write_to_sf(df, schema, table, database, warehouse, s3_bucket)

    except Exception as e:
        print(f'Failure: {e}')
        sys.exit()

    finally:
        print('-------------------------------------')
        print('Finish: Pandas Dataframe to Snowflake')
        time_stop = process_time()
        print(f'Elapsed time in seconds: {(time_stop - time_start)}')
        print('-------------------------------------')
        sys.exit()