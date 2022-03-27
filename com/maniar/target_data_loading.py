from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.utils.utilities as ut

if __name__ == '__main__':

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_conf = app_conf['s3_conf']
    datalake_path = 's3a://' + s3_conf['s3_bucket'] + '/' + s3_conf['datalake_path']


    target_list=app_conf["target_list"]

    for tgt in target_list:

        src_conf=app_conf[tgt]["source_data"]

        if (tgt=='REGIS_DIM'):
            print("\nReading Customer Data")
            cp_df=spark.read\
                    .parquet(datalake_path+"/"+src_conf)

            cp_df.createOrReplaceTempView("DATAMART.REGIS_DIM")




