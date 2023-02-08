import os
import snowflake.connector
import logging
# import pendulum

from pandas import read_sql, DataFrame
# from src.etl.lib.s3_utils import AnbS3BucketUtils, get_s3_bucket_credentials
# from src.etl.lib.log_utils import get_logger

# logger = get_logger(__name__)


def get_sf_conn() -> snowflake.connector:
	'''Returns a snowflake connection object.'''
	logging.info('returning snowflake connector')
	return snowflake.connector.connect(user=os.getenv('KIVA_USERNAME'),
									   password=os.getenv('KIVA_PASSWORD'),
									   account=os.getenv('KIVA_ACCOUNT'),
									   warehouse=os.getenv('KIVA_WAREHOUSE'),
									   database=os.getenv('KIVA_DATABASE'),
									   role=os.getenv('KIVA_ROLE'))

def get_df(sql: str) -> DataFrame:
	'''Takes a SQL statement and returns a DataFrame.'''
	logging.info('getting dataframe from snowflake')
	with get_sf_conn() as conn:
		return read_sql(sql, conn)

def get_views(schema):
    sql = f"""
        show views in {schema};
        """
    df = get_df(sql)
    print(df)

def get_table_data(table, start_date, end_date):
    sql = f"""
        select * from {table} 
        where date_trunc('day', start_time) between {start_date} and {end_date} 
        order by start_time asc;
    """
    df = get_df(sql)
    print(df)

if __name__=='__main__':
    schema = 'kiva_prod.optiml'
    table = 'kiva_prod.optiml.warehouse_metering_history'
    get_views(schema)





# def execute_sql(sql: str) -> None:
# 	logger.info('executing sql')
# 	conn = get_sf_conn()
# 	with conn.cursor() as curs:
# 		curs.execute(sql)


# def write_df_to_snowflake(df, sf_table_name) -> None:
# 	'''Takes a pandas dataframe and writes to a target Snowflake table.'''
# 	logger.info(f'writing dataframe to snowflake table {sf_table_name}')
# 	ts = pendulum.now().format('YYYYMMDD_HHmmssSSS')
# 	temp_filename = '{0}_{1}.csv'.format(sf_table_name, ts)
# 	try:

# 		logger.info('creating temporary csv file')
# 		df.to_csv(temp_filename, index=False)

# 		logger.info('uploading temp csv file to s3')
# 		s3 = AnbS3BucketUtils()
# 		s3.upload_file(temp_filename)

# 		logger.info('constructing COPY INTO command')
# 		aws_creds = get_s3_bucket_credentials()
		
# 		copy_into_sql = '''
# 		COPY INTO {target_sf_table_name} 
# 	    FROM 'S3://{s3_bucket}/'
# 	    CREDENTIALS = (AWS_KEY_ID = '{aws_access}' AWS_SECRET_KEY = '{aws_secret}')
# 	    FILES = ('{key_name}')
# 	    FILE_FORMAT = (TYPE = csv
# 	    			   SKIP_HEADER = 1)
# 		;
# 		'''.format(target_sf_table_name=sf_table_name,
# 				   s3_bucket=aws_creds['s3_bucket'],
# 				   aws_access=aws_creds['aws_access'],
# 				   aws_secret=aws_creds['aws_secret'],
# 				   key_name=temp_filename)

# 		with get_sf_conn() as conn:

# 			logger.info('executing COPY INTO command')
# 			curs = conn.cursor()
# 			curs.execute(copy_into_sql)

# 	except Exception as e:
# 		logger.error(f'write_df_to_snowflake error: {e}')
# 		raise e

# 	finally:
# 		logger.info('deleting local & s3 copies of temp csv file')
# 		os.remove(temp_filename)
# 		s3.delete_file(temp_filename)