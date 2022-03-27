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

    target_list=[]
    target_list.append(app_conf["target_list"])

    for tgt in target_list:
        tgt_conf=app_conf[tgt]

        if (tgt=='REGIS_DIM'):
            src_conf = tgt_conf["source_data"]
            print("\nReading Customer Data")
            cp_df=spark.read\
                    .parquet(datalake_path+"/"+src_conf)

            cp_df.createOrReplaceTempView("CP")

            cp_tgt_df=spark.sql(tgt_conf["loadingQuery"])

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            cp_tgt_df.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.REGIS_DIM") \
                .mode("overwrite") \
                .save()


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/target_data_loading.py
