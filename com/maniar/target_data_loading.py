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
        src_conf = tgt_conf["source_data"]

        if (tgt=='REGIS_DIM'):
            print("\nReading Customer Data")
            cp_df=spark.read\
                    .parquet(datalake_path+"/"+src_conf)

            cp_df.createOrReplaceTempView("CP")

            cp_tgt_df=spark.sql(tgt_conf["loadingQuery"])

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            ut.write_to_RedShift(cp_tgt_df, jdbc_url, app_conf, "DATAMART.REGIS_DIM")

        elif tgt == 'CHILD_DIM':
            print("\nReading Customer Child Data")
            cp_df = spark.read \
                .parquet(datalake_path + "/" + src_conf)

            cp_df.createOrReplaceTempView("CP")

            cp_tgt_df = spark.sql(tgt_conf["loadingQuery"])

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            ut.write_to_RedShift(cp_tgt_df, jdbc_url, app_conf, "DATAMART.CHILD_DIM")


        elif tgt == 'RTL_TXN_FCT':
            print("\nReading SB(Smart Button) Data")
            sb_df = spark.read \
                .parquet(datalake_path + "/" + src_conf[0])
            sb_df.createOrReplaceTempView("SB")
            sb_df.show()

            print("\nReading OL(Orchestration Layer) Data")
            ol_df = spark.read \
                .parquet(datalake_path + "/" + src_conf[1])
            ol_df.createOrReplaceTempView("OL")
            ol_df.show()

            print("\nReading REGIS_DIM Data")
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            dim_df = spark.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("dbtable", app_conf[tgt]["target_src_table"]) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .load()
            dim_df.createOrReplaceTempView("REGIS_DIM")
            dim_df.show()

            tgt_df = spark.sql(tgt_conf["loadingQuery"])

            tgt_df.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.RTL_TXN_FCT") \
                .mode("overwrite") \
                .save()



        # spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/target_data_loading.py
# --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/target_data_loading.py
