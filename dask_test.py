from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.dataframe as dd # Utilizado para el procesamiento de los datos

cluster = YarnCluster(environment='conf/conda_envs/dask_yarn.tar.gz',
                      worker_vcores=2,
                      worker_memory="8GiB")

cluster.scale(2)

# if __name__ == '__main__':

client = Client(cluster)
path = 'hdfs:///data_10K_dask_casted/'

df = dd.read_parquet(path, engine='pyarrow')

print(df.count().compute())
