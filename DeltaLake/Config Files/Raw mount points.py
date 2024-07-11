# Databricks notebook source
secret_id = dbutils.secrets.get(scope = "scoperaunak", key = "devclient-id")
devclient_secret = dbutils.secrets.get(scope = "scoperaunak", key = "devclient-secret")
tenant_id = dbutils.secrets.get(scope = "scoperaunak", key = "tenant-id")
source_gen2_link= dbutils.secrets.get(scope = "scoperaunak", key = "source-gen2-link")

# COMMAND ----------

if secret_id =='9c5c9982-003c-4e76-bfb4-acbe7133b69c':
    print("secret is correct")
else:
    print("secret is not correct")

# COMMAND ----------

if devclient_secret =='DL~8Q~XF8Am73-Z4efhYXt~3Bj1NZJHnzljRNbTI':
    print("devclient_secret is correct")
else:
    print("devclient_secret is not correct")

# COMMAND ----------

if tenant_id =='7597c8e2-3238-48ff-a99b-1e16c07830d8':
    print("tenant_id is correct")
else:
    print("tenant_id is not correct")

# COMMAND ----------

if source_gen2_link =='abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net/':
    print("source_gen2_link is correct")
else:
    print("source_gen2_link is not correct")

# COMMAND ----------

dbutils.secrets.get(scope = "raunak-deltalake", key = "source-gen2-link")

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AzureDLGen2Access") \
    .getOrCreate()

# Set the Spark configuration for accessing ADLS Gen2
spark.conf.set("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<your-access-key>")

# Example to read a file from ADLS Gen2
df = spark.read.format("csv").option("header", "true").load("abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net/circuit/circuits_2023_07_29.csv")

# Show the DataFrame
df.show()


# COMMAND ----------

def f_get_secret(key):
    try:
        return dbutils.secrets.get(scope = "raunak-deltalake", key = key)
    except Exception as err:     
        print("error Occured",str(err))

#abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net/
#abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net/
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org-apache.hadoop.fs.azurebfs.outh2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f_get_secret(key = 'devclient-id'),
           "fs.azure.account.oauth2.client.secret": f_get_secret(key = 'devclient-secret'),
           "fs.azure.account.oauth2.client.endpoint":f_get_secret(key = 'tenant-id')
           }
           


# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org-apache.hadoop.fs.azurebfs.outh2.ClientCredsTokenProvider",
           "fs.azure.account.outh2.client.id": dbutils.secrets.get(scope='scoperaunak', key='devclient-id'),
           "fs.azure.account.outh2.client.secret": dbutils.secrets.get(scope='scoperaunak', key='devclient-secret'),
           "fs.azure.account.outh2.client.endpoint": dbutils.secrets.get(scope='scoperaunak', key='tenant-id')
           }


# COMMAND ----------

mountPoint=
    source=f_get_secret( "source-gen2-link"),
    #source = "Wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
    mount_point = "/mnt/sourcestoragegen2acc/",
dbutils.fs.mount(
extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get (scope =
"<scope-name>", key =

# COMMAND ----------


mountPoint="/mnt/sourcestoragegen2acc/"
#optionally ,you can provide add<directory-name> to the source URI of your mount point
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = f_get_secret( "source-gen2-link"), #path for the blob link
        mount_point = mountPoint,
        extra_configs = configs)


# COMMAND ----------


dbutils.fs.ls('/mnt/sourcestoragegen2acc/')

# COMMAND ----------

#dbutils.fs.mounts()

# COMMAND ----------

mountPoint = "/mnt/sourcestoragegen2acc/"
#abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net/
# Optionally, you can provide a directory name to the source URI of your mount point
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source= dbutils.secrets.get(scope='scoperaunak', key='tenant-id'),  # Path for the blob link
        mount_point=mountPoint,
        extra_configs=configs
    )


# COMMAND ----------

