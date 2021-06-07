# Pasos para ejecutar algoritmos en un clúster de Hadoop

## Clúster en AWS

### Creación y acceso al clúster 

Para crear un clúster son necesarios dos archivos de configuración, tener instalado `aws cli` y tener un usuario de `AWS` con los permisos adecuados.

Para la creación del clúster se utilizó el script ubicado en `conf/dask_prep.sh` y fue cargado a un _bucket_ de _Amazon S3_. La ruta a la que fue cargada se debe de escribir en el espacio `<ruta_de_script_a_s3>`. El objetivo de este script es clonar el repositorio de _GitHub_ en cada uno de los nodos y hacer la instalación de los paquetes necesarios para la ejecución de los procesos. En caso de que falle en alguno de los nodos, este archivo se puede ejecutar manualmente con el comando `sh dask_prep.sh`.

Además, se utilizó un archivo de configuración para cambiar el calculador de recursos del predeterminado (`DefaultResourceCalculator`) a uno que permite que el usuario especifique los recursos a utilizar (`DominantResourceCalculator`). Este archivo se almacenó en la ruta `conf/dask_cluster/configuration.json` y su contenido es el siguiente:
```
[{
    "Classification":"capacity-scheduler",
    "Properties":
    {
        "yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}]
```

Una vez generados los archivos anteriores podemos ejecutar el siguiente comando que iniciará la creación del clúster con las siguientes características:

- _Hadoop_ `Amazon 2.10.1`.
- `Spark 2.4.7`
- 1 nodo _master_ alojado en una instancia `m5a.xlarge` con 4 cores, 16 GB de RAM y 64 GB de almacenamiento mas 10 GB para la partición _root_.
- 4 nodos _worker_ alojados en instancias `m5a.2xlarge` con 8 cores, 32 GB de RAM y 128 GB de almacenamiento mas 10 GB para la partición _root_.

```
aws emr create-cluster --applications Name=Hadoop Name=Spark --ebs-root-volume-size 10 \
    --ec2-attributes '{"KeyName":"rita-transtat","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-6f33f035","EmrManagedSlaveSecurityGroup":"sg-0b109e42b0571bd97","EmrManagedMasterSecurityGroup":"sg-0504e18a54e06e6e1"}' \
    --service-role EMR_DefaultRole \
    --release-label emr-5.33.0 \
    --name 'transtat-cluster' \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --instance-fleets '[{"InstanceFleetType":"MASTER","TargetOnDemandCapacity":1,"TargetSpotCapacity":0,"LaunchSpecifications":{},"InstanceTypeConfigs":[{"WeightedCapacity":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"BidPriceAsPercentageOfOnDemandPrice":100,"InstanceType":"m5a.xlarge"}],"Name":"Master - 1"},{"InstanceFleetType":"CORE","TargetOnDemandCapacity":24,"TargetSpotCapacity":0,"LaunchSpecifications":{},"InstanceTypeConfigs":[{"WeightedCapacity":8,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"BidPriceAsPercentageOfOnDemandPrice":100,"InstanceType":"m5a.2xlarge"}],"Name":"Core - 2"}]' \
    --region us-west-1 \
    --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://us-west-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["<ruta_de_script_a_s3>"] \
    --configurations file://./conf/dask_cluster/configuration.json
```

Después de unos 15 minutos aproximadamente el clúster pasará a los estados _waiting_ o _running_. A partir de ese momento podemos conectarnos mediante `ssh` con un comando similar a este:
```
ssh -i ~/credenciales.pem hadoop@xxxxxxxxxxxxxxxx.nombre_de_la_region.compute.amazonaws.com
```

Para este paso es importante tener credenciales de acceso en un archivo credenciales.pem con los permisos necesarios y haber habilitado nuestra dirección ip para tener acceso al clúster.

### Descarga de datos

El siguiente paso es transferir datos almacenados en `s3` al sistema de almacenamiento del clúster (`hdfs`). Para ello podemos seguir los siguientes pasos suponiendo que los datos necesarios ya están en el clúster.

1. Crear el directorio `/samples` para almacenar los datos con el comando `hdfs dfs -mkdir /samples`.
2. Copiar los datos del bucket con el comando: `s3-dist-cp --src=s3://directorio/origen --dest=hdfs://directorio/destino`. Por ejemplo, para copiar las diferentes muestras del clúster se utilizaron los siguientes comandos:
```
hdfs dfs -mkdir /user/samples/

s3-dist-cp --src=s3://spark-rita/samples/data_10K_dask_casted --dest=hdfs:///samples/data_10K_dask_casted
s3-dist-cp --src=s3://spark-rita/samples/data_10K_spark_casted --dest=hdfs:///samples/data_10K_spark_casted

s3-dist-cp --src=s3://spark-rita/samples/data_100K_dask_casted --dest=hdfs:///samples/data_100K_dask_casted
s3-dist-cp --src=s3://spark-rita/samples/data_100K_spark_casted --dest=hdfs:///samples/data_100K_spark_casted

s3-dist-cp --src=s3://spark-rita/samples/data_1M_dask_casted --dest=hdfs:///samples/data_1M_dask_casted
s3-dist-cp --src=s3://spark-rita/samples/data_1M_spark_casted --dest=hdfs:///samples/data_1M_spark_casted

s3-dist-cp --src=s3://spark-rita/samples/data_10M_dask_casted --dest=hdfs:///samples/data_10M_dask_casted
s3-dist-cp --src=s3://spark-rita/samples/data_10M_spark_casted --dest=hdfs:///samples/data_10M_spark_casted

s3-dist-cp --src=s3://spark-rita/samples/data_full_dask_casted --dest=hdfs:///samples/data_full_dask_casted
s3-dist-cp --src=s3://spark-rita/samples/data_full_spark_casted --dest=hdfs:///samples/data_full_spark_casted
```

Después de este paso el clúster está listo para ejecutar los procesos.

## Clúster en Azure

Los siguientes pasos describen cómo hacer un clúster similar al de la sección anterior pero en Azure. Debido a las diferencias en la plataforma estos pueden ser distintos.

### Creación del clúster

Para crear el clúster, la forma más sencilla es ir a la consola de administración de `Azure` e ingresar al servicio `HD Insight`. Una vez que estemos ahí hay que seleccionar la opción `create cluster` y seguir el `wizard`. Es importante seleccionar la región en la que estén alojados nuestros datos y usar el contenedor de `Azure Blob Sotrage` que los almacene como el contenedor predeterminado del clúster.

El clúster creado 

Una vez creado nos dará la opción de conectarnos por `ssh` con las credenciales que definimos durante el wizard.

### Instalación de miniconda y paquetes necesarios

Después de hacer la conexión al clúster mediante `ssh` debemos de instalar miniconda y los paquetes necesarios en cada nodo. Para ello debemos copiar el archivo ``.

1. Descargar el archivo de instalación de miniconda y ejecutar el archivo de instalación de miniconda con el siguiente comando: `wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh; sh Miniconda3-latest-Linux-x86_64.sh`.
2. Seguir los pasos del instalador y salir de la consola y volver a entrar por `ssh` para que se efectuen los cambios.
3. Ejecutar el comando: `conda config --set auto_activate_base false` para evitar que el ambiente de anaconda se active solo.
4. Salir del ambiente con `conda deactivate`.
5. Después clonamos el respositorio con el siguiente comando: `git clone https://ikerforce:<token_de_github>@github.com/ikerforce/spark-rita.git`.
6. Entrar al repositorio con el comando `cd spark-rita`.
6. Crear el ambiente a partir del archivo de configuración con el comando: `conda env create -f conf/conda_envs/dask_yarn.yml`.

Con estos pasos ya está listo el ambiente de ejecución de `dask`.

### Empaquetar el ambiente de anaconda

Para ejecutar el trabajo en todos los nodos, es necesario ejecutar el siguiente comando que empaqueta el ambiente de anaconda.

```
conda-pack -o conf/conda_envs/dask_yarn.tar.gz
```

Es importante que estemos dentro del ambiente que queremos empaquetar.


### Cambiar path de hdfs en Ambari

El path default de amabri es hacia Blob Storage y no al hdfs local del clúster, por lo que en ambari hay que cambiar el valor de `fs.defaultFS` en la sección _Advanced core-site_ por la opción `hdfs://mycluster/`.

### Descarga de datos

Una vez realizado el cambio de hdfs hay que seguir los siguientes pasos:

1. Crear una carpeta para almacenar información del ambiente: `hdfs dfs -mkdir /conda_envs/`.
2. Crear una carpeta para almacenar datos: `hdfs dfs -mkdir /samples/`.
3. Copiar el ambiente de anaconda a hdfs con el comando: `hdfs dfs -copyFromLocal conf/conda_envs/dask_yarn.tar.gz /conda_envs/`.
4. Copiar datos de blob storage a hdfs con el comando: `hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_dask_casted /samples/`.

### Cambiar la versión default de python a la 2.7 (comentar en .bashrc al final lo de anaconda)

```
hdfs dfs -mkdir /samples/
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_dask_casted hdfs://mycluster/samples/data_100K_dask_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_spark_casted hdfs://mycluster/samples/data_100K_spark_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_1M_dask_casted hdfs://mycluster/samples/data_1M_dask_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_1M_spark_casted hdfs://mycluster/samples/data_1M_spark_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_10M_dask_casted hdfs://mycluster/samples/data_10M_dask_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_10M_spark_casted hdfs://mycluster/samples/data_10M_spark_casted
hdfs dfs -mkdir hdfs://mycluster/spark/
hdfs dfs -mkdir hdfs://mycluster/spark/events/


hdfs dfs -mkdir hdfs://mycluster/user/sshuser/samples/
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_dask_casted hdfs://mycluster/user/sshuser/samples/data_100K_dask_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_spark_casted hdfs://mycluster/user/sshuser/samples/data_100K_spark_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_1M_dask_casted hdfs://mycluster/user/sshuser/samples/data_1M_spark_casted
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_1M_spark_casted hdfs://mycluster/user/sshuser/samples/data_1M_spark_casted
hdfs dfs -mkdir hdfs://mycluster/user/sshuser/spark/
hdfs dfs -mkdir hdfs://mycluster/user/sshuser/spark/events/

```

Cambiar en Ambari las propiedades `Spark Eventlog directory` y `Spark History FS Log directory` por `hdfs://mycluster/spark/events/` en la sección _Advanced spark2-defaults_
Cambiar en Ambari las propiedades `Spark Eventlog directory` y `Spark History FS Log directory` por `hdfs://mycluster/spark/events/` en la sección Advanced _spark2-thrift-sparkconf_


nohup dask-scheduler > dask_scheduler.out &
cat dask_scheduler.out

nohup dask-worker tcp://172.31.12.168:8786 &> dask_worker.out &
cat dask_worker.out

nohup python -u src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 10 --sample_size 10M --env cluster --scheduler tcp://172.31.12.168:8786 &> ejecuciones_10M_$(date +%Y-%m-%d-%Hh%Mm%Ss).out &

python src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 1 --sample_size 100K --env cluster --scheduler tcp://172.31.0.11:8786

nohup python -u src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 1 --sample_size 100K --scheduler 10.0.0.18:8786 --env cluster &> ejecuciones.out &

nohup python -u src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 10 --sample_size 100K --scheduler 10.0.0.23:8786 --env cluster &> ejecuciones_100K_$(date +%Y-%m-%d-%Hh%Mm%Ss).out &

nohup python -u src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 100 --sample_size 10M --scheduler 10.0.0.23:8786 --env cluster &> ejecuciones_10M_$(date +%Y-%m-%d-%Hh%Mm%Ss).out &

python src/rita_ejecuciones.py --creds conf/mysql_creds.json --ejecs 1 --sample_size 1M --scheduler tcp://172.31.8.8:8786 --env cluster

aws emr create-cluster --name "Test cluster" --release-label emr-5.33.0 --applications Name=Hive Name=Pig --use-default-roles --ec2-attributes KeyName=myKey --instance-type m5.xlarge --instance-count 3 --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://region.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://mybucket/script-path/my_script.sh"]

scp conf/mysql_creds.json sshuser@10.0.0.4:spark-rita/conf/

mysql -u transtat@transtat-rita -h transtat-rita.mysql.database.azure.com -p TRANSTAT

mysql -u transtat -h transtat-rita.caevbcmhveep.us-west-1.rds.amazonaws.com -p TRANSTAT -P 3306


aws s3 cp samples/data_10M_dask_casted s3://spark-rita/samples/data_10M_dask_casted --recursive

aws s3 cp samples/data_full_spark_casted s3://spark-rita/samples/data_full_spark_casted --recursive

aws s3 sync samples/data_10M_dask_casted s3://spark-rita/samples/data_10M_dask_casted --recursive

aws s3 cp samples/data_full_spark_casted s3://spark-rita/samples/data_full_spark_casted --recursive

aws s3 cp samples/data_full_dask_casted s3://spark-rita/samples/data_full_dask_casted --recursive

spark-submit     --driver-memory=2G     --driver-cores=2     --num-executors=3     --executor-cores=3     --executor-memory=4G     --jars sql/mysql-connector-java-8.0.23.jar     src/rita_master_spark.py     --env cluster     --creds conf/mysql_creds.json     --process demoras_ruta_mktid_spark     --sample_size 1M


spark-submit \
    --driver-memory=2G \
    --driver-cores=2 \
    --num-executors=2 \
    --executor-cores=3 \
    --conf spark.sql.shuffle.partitions=20 \
    --executor-memory=6G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_origen_spark \
    --command_time 0.0 \
    --sample_size 100K


python \
    src/rita_master_dask.py \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_origen_dask \
    --sample_size 100K


python \
    src/rita_master_dask.py \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_dask \
    --sample_size 100K

spark-submit \
    --driver-memory=11G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_spark \
    --sample_size 100K


spark-submit \
    --driver-memory=11G \
    --conf spark.sql.shuffle.partitions=12 \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_spark \
    --command_time 0.0 \
    --sample_size 1M


spark-submit \
    --driver-memory=8G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_origen_spark \
    --sample_size 100K


334 tardaba

spark-submit \
    --driver-memory 2G \
    --driver-cores 2 \
    --num-executors 2 \
    --executor-cores 3 \
    --executor-memory 6G \
    --conf spark.sql.queryExecutionListeners="" \
    --conf spark.sql.extensions="" \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/calculo_ruta_minima/dijkstra_spark.py \
    --env cluster \
    --sample_size 100K \
    --process dijkstra_spark \
    --creds conf/mysql_creds.json \
    --origin CLE \
    --dest HNL


spark-submit \
    --driver-memory 8G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/calculo_ruta_minima/dijkstra_spark_pandas.py \
    --env local \
    --sample_size 100K \
    --process dijkstra_spark \
    --creds conf/mysql_creds.json \
    --origin CLE \
    --dest HNL


spark-submit \
    --driver-memory 8G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/calculo_ruta_minima/dijkstra_spark_pandas.py \
    --env local \
    --sample_size 1M \
    --process dijkstra_spark \
    --creds conf/mysql_creds.json \
    --origin AVP \
    --dest LAS


spark-submit \
    --driver-memory 8G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/calculo_ruta_minima/dijkstra_spark.py \
    --env local \
    --sample_size 100K \
    --process dijkstra_spark \
    --creds conf/mysql_creds.json \
    --origin CLE \
    --dest HNL


python \
    src/calculo_ruta_minima/dijkstra_dask.py \
    --creds conf/mysql_creds.json \
    --process dijkstra_dask \
    --sample_size 10M \
    --origin CLE \
    --dest HNL

spark-submit \
    --driver-memory=2G \
    --driver-cores=2 \
    --num-executors=20 \
    --executor-cores=3 \
    --executor-memory=6G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_destino_spark \
    --sample_size 1M

spark-submit \
    --driver-memory=2G \
    --driver-cores=2 \
    --num-executors=4 \
    --executor-cores=3 \
    --executor-memory=11G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process flota_spark \
    --sample_size 1M

spark-submit \
    --driver-memory=2G \
    --driver-cores=2 \
    --num-executors=5 \
    --executor-cores=1 \
    --executor-memory=2G \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process elimina_nulos_spark \
    --sample_size 1M

spark-submit \
    --master local \
    --driver-memory=8g \
    --files log4j.properties \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_destino_spark \
    --sample_size 10M


spark-submit \
    --master local \
    --driver-memory=8g \
    --files log4j.properties \
    --conf spark.sql.shuffle.partitions=50 \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_aeropuerto_destino_spark \
    --sample_size 10M


spark-submit \
    --jars sql/mysql-connector-java-8.0.23.jar \
    src/rita_master_spark.py \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process elimina_nulos_spark \
    --sample_size 1M

python \
    src/rita_master_dask.py \
    --creds conf/mysql_creds.json \
    --process demoras_aerolinea_dask \
    --sample_size 1M

python \
    src/rita_master_dask.py \
    --scheduler tcp://192.168.3.46:8786 \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_dask \
    --command_time 0.0 \
    --sample_size 100K


python \
    src/rita_master_dask.py \
    --scheduler tcp://172.31.12.168:8786 \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_dask \
    --command_time 0.0 \
    --sample_size 100K

python \
    src/rita_master_dask.py \
    --env local \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_dask \
    --command_time 0.0 \
    --sample_size 100K


mysql -u <local database username> -h <database server ip address> -p



from dask import dataframe as dd
df = dd.read_parquet('hdfs:///samples/data_10M_dask_casted/*')
import time
s = time.time()
df['CRS_ELAPSED_TIME'].mean().compute()
print(time.time() - s)
print('ok')



from dask import dataframe as dd
df = dd.read_parquet('hdfs:///samples/data_10M_dask_casted/*.parquet')
import time
s = time.time()
df['DIFERENCIA'] = df['CRS_ELAPSED_TIME'] - df['ACTUAL_ELAPSED_TIME']
df['DIFERENCIA'].mean().compute()
df['DIFERENCIA'].max().compute()
df['DIFERENCIA'].min().compute()
df['DIFERENCIA'].std().compute()
print(time.time() - s)
print('ok')


from dask import dataframe as dd
df = dd.read_parquet('hdfs:///samples/data_10M_dask_casted/*')
import time
s = time.time()
df['DIFERENCIA'] = df['CRS_ELAPSED_TIME'] - df['ACTUAL_ELAPSED_TIME']
df['DIFERENCIA'].mean().compute()
df['DIFERENCIA'].max().compute()
df['DIFERENCIA'].min().compute()
df['DIFERENCIA'].std().compute()
print(time.time() - s)
print('ok')


python3 \
    src/rita_master_dask.py \
    --scheduler tcp://172.31.12.168:8786 \
    --env cluster \
    --creds conf/mysql_creds.json \
    --process demoras_ruta_mktid_dask \
    --command_time 0.0 \
    --sample_size 1M


ssh -i conf/rita-transtat.pem hadoop@ec2-13-52-103-94.us-west-1.compute.amazonaws.com