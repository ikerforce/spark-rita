import os
os.environ['ARROW_LIBHDFS_DIR'] = '/usr/hdp/4.1.4.0/'

from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.dataframe as dd # Utilizado para el procesamiento de los datos

cluster = YarnCluster(environment='conf/conda_envs/dask_yarn.tar.gz',
                      worker_vcores=4,
                      worker_memory="2GB")

print(cluster.application_client.get_containers())

cluster.scale(2)

print(cluster)

# if __name__ == '__main__':

client = Client(cluster)
path = 'hdfs:///samples/data_100K_dask_casted/data_100K_dask_casted'

print(client)

df = dd.read_parquet(path)

print(df.count().compute())
