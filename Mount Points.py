# Databricks notebook source
# Function to fetch secret
#mountpoint for source
def f_get_secret(key):
    return dbutils.secrets.get(scope="scoperaunak", key=key)

# Configuration dictionary with OAuth credentials
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f_get_secret(key="devclient-id"),
    "fs.azure.account.oauth2.client.secret": f_get_secret(key="devclient-secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{f_get_secret(key='tenant-id')}/oauth2/token" # Ensure this is the full URL with protocol
}
#mountPoint = '/mnt/sourcestorageblobacc'
#abfss://raunakcontainer@sourceblobraunaksa.blob.core.windows.net/
# Unmount if already mounted
#if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)
# if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#    dbutils.fs.mount(
#    source='abfss://<container>@<accountname>.dfs.core.windows.net'  # Ensure this is the full URI with protocol
#    mount_point=mountPoint,
#    extra_configs=configs
#)

# COMMAND ----------

mountPoint = '/mnt/sourcestorageblobacc'
#abfss://raunakcontainer@sourceblobraunaksa.blob.core.windows.net/
# Unmount if already mounted
#if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source='abfss://raunakcontainer@sourceblobraunaksa.dfs.core.windows.net/', abfss://<container>@<accountname>.dfs.core.windows.net  # Ensure this is the full URI with protocol
    mount_point=mountPoint,
    extra_configs=configs
)

# COMMAND ----------


mountPoint = '/mnt/sourcestoragegen2acc'

# Unmount if already mounted
if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mountPoint)

# Mount the directory
dbutils.fs.mount(
    source="abfss://sourceraunak@sourcestoragegen2acc.dfs.core.windows.net",  # Ensure this is the full URI with protocol
    mount_point=mountPoint,
    extra_configs=configs
)

# COMMAND ----------

# Function to fetch secret
#mount point for sink layer gen2
mountPoint = '/mnt/bronze_sinkstoragegen2acc'

# Unmount if already mounted
#if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source=f_get_secret(key='bronze-gen2-link'),  # Ensure this is the full URI with protocol
    mount_point=mountPoint,
    extra_configs=configs
)

# COMMAND ----------

# Function to fetch secret
#mount point for sink layer gen2
mountPoint = '/mnt/silver_sinkstoragegen2acc'
#abfss://silver@deltaraunak.dfs.core.windows.net/
# Unmount if already mounted
#if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source=f_get_secret(key='silver-gen2-link'),  # Ensure this is the full URI with protocol
    mount_point=mountPoint,
    extra_configs=configs
)

# COMMAND ----------

# Function to fetch secret
#mount point for sink layer gen2
mountPoint = '/mnt/gold_sinkstoragegen2acc'
#abfss://gold@deltaraunak.dfs.core.windows.net/
# Unmount if already mounted
#if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source=f_get_secret(key='gold-gen2-link'),  # Ensure this is the full URI with protocol
    mount_point=mountPoint,
    extra_configs=configs
)

# COMMAND ----------

dbutils.fs.ls('/mnt/sourcestoragegen2acc')

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze_sinkstoragegen2acc')

# COMMAND ----------

dbutils.fs.ls('/mnt/silver_sinkstoragegen2acc')

# COMMAND ----------

dbutils.fs.ls('/mnt/gold_sinkstoragegen2acc')

# COMMAND ----------

dbutils.fs.ls('/mnt/sourcestorageblobacc')

# COMMAND ----------

