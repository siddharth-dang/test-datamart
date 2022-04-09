def read_from_sftp(spark, hostname, port, username, keyfile_path, filename):
    df = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", hostname) \
        .option("port", port) \
        .option("username", username ) \
        .option("pem",keyfile_path) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(filename)
    return df

def get_mysql_jdbc_url(hostname, port, database):
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(hostname, port, database)

def read_from_mysql(spark, hostname, port, database, query, partition_column, username, password):
    jdbc_params = {"url": get_mysql_jdbc_url(hostname, port, database),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": query,
                   "numPartitions": "2",
                   "partitionColumn": partition_column,
                   "user": username,
                   "password": password
                   }
    # print(jdbcParams)

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txnDF = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()

    return txnDF


def read_from_mongoDB(spark, database, collection,uri):
    address_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri",uri)\
        .option("database", database) \
        .option("collection", collection) \
        .load()

    return address_df


def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)

def read_from_s3(spark, src_conf, delimiter = "|", header = "true"):
    df = spark.read \
        .option("header", header) \
        .option("delimiter", delimiter) \
        .csv('s3a://' + src_conf['s3_conf']['s3_bucket'] + "/" + src_conf['filename'])
    return df

def write_to_s3(df, datalake_path, src, partition_column = 'ins_dt'):
    df.write.mode('append').partitionBy('ins_dt').parquet(datalake_path + '/' + src)