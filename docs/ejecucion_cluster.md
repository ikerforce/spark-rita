# Pasos para ejecutar algoritmos en un clúster de Hadoop

## Clúster en AWS

### Creación y acceso al clúster 

El primer paso es utilizar el siguiente comando para crear un clúster. Para esto es necesario tener instalado `aws cli` y tener un usuario con los permisos adecuados.

```
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --applications Name=Hadoop Name=Hive Name=Spark --ebs-root-volume-size 20 --ec2-attributes '{"KeyName":"rita-transtat","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-6f33f035","EmrManagedSlaveSecurityGroup":"sg-0b109e42b0571bd97","EmrManagedMasterSecurityGroup":"sg-0504e18a54e06e6e1"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.33.0 --log-uri 's3n://aws-logs-179081443845-us-west-1/elasticmapreduce/' --name 'My cluster' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-1
```


Una vez que el clúster esté en el estado _waiting_ o _running_, podemos conectarnos mediante `ssh` con un comando similar a este:

```
ssh -i ~/credenciales.pem hadoop@xxxxxxxxxxxxxxxx.nombre_de_la_region.compute.amazonaws.com
```

Para este paso es importante tener credenciales de acceso en un archivo credenciales.pem con los permisos necesarios y haber habilitado nuestra dirección ip para tener acceso al clúster.


### Instalación de miniconda y paquetes necesarios

Después de hacer la conexión al clúster mediante `ssh` debemos de instalar miniconda y los paquetes necesarios con los siguientes comandos:

1. Descargar el archivo de instalación de miniconda con el siguiente comando: `wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh`.
2. Ejecutar el archivo de instalación de miniconda con el siguiente comando: `sh Miniconda3-latest-Linux-x86_64.sh`.
3. Seguir los pasos del instalador y salir de la consola y volver a entrar por `ssh` para que se efectuen los cambios.
4. Crear un ambiente de anaconda con el comando `conda create -n dask_yarn` y activarlo con el comando `conda activate dask_yarn`.
5. Instalar paquetes de `conda-forge` con el comando `conda install -c conda-forge dask-yarn libhdfs3 conda-pack sqlalchemy fsspec`.
6. Instalar paquetes restantes con el comando `conda install -c anaconda pandas dask numpy`.
7. Instalar `pyarrow` con `pip` mediante el comando `pip3 install pyarrow`.
8. Instalar `s3fs` con el comando: `conda install -c conda-forge s3fs`.

Con estos pasos ya está listo el ambiente de ejecución de `dask`, ahora vamos a descargar el repositorio.

### Descarga de recursos del repositorio remoto

Los siguientes pasos permitirán ingresar al repositorio y usar el código y los rescursos que contiene.

1. Primero hay que instalar git en el nodo con el comando: `sudo yum install git-core`.
2. Después clonamos el respositorio con el siguiente comando: `git clone https://ikerforce:<tu_token_de_github>@github.com/ikerforce/spark-rita.git`.

Una vez finalizados los pasos anteriores, ya podrás usar el repositorio.

### Descarga de datos

El siguiente paso es transferir los datos de `s3` al `hdfs` local.

1. Crear el directorio `/samples` para almacenar los datos con el comando `hdfs dfs -mkdir /samples`.
2. Copiar los datos del bucket con el comando: `s3-dist-cp --src=s3://directorio/origen --dest=hdfs://directorio/destino`, por ejemplo lo podemos usar para la carpeta `data_10K_dask_casted`: `s3-dist-cp --src=s3://spark-rita/samples/data_10K_dask_casted --dest=hdfs:///samples/data_10K_dask_casted`.

### Empaquetar el ambiente de anaconda

Para ejecutar el trabajo en todos los nodos, es necesario ejecutar el siguiente comando que empaqueta el ambiente de anaconda.

```
conda-pack -o conf/conda_envs/dask_yarn.tar.gz
```

Es importante que estemos dentro del ambiente que queremos empaquetar.


### Clúster en Azure

## Creación y acceso

```
az group create \
    --location "southcentralus" \
    --name "test-cluster"

azure group deployment create -f deployment.json -g "test-cluster" -n "rita-transtat-c"
```


### Instalación de miniconda y paquetes necesarios

Después de hacer la conexión al clúster mediante `ssh` debemos de instalar miniconda y los paquetes necesarios con los siguientes comandos:

1. Descargar el archivo de instalación de miniconda y ejecutar el archivo de instalación de miniconda con el siguiente comando: `wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh; sh Miniconda3-latest-Linux-x86_64.sh`.
2. Seguir los pasos del instalador y salir de la consola y volver a entrar por `ssh` para que se efectuen los cambios.
3. Ejecutar el comando: `conda config --set auto_activate_base false` para evitar que el ambiente de anaconda se active solo.
4. Salir del ambiente con `conda deactivate`.
5. Después clonamos el respositorio con el siguiente comando: `git clone https://ikerforce:<tu_token_de_github>@github.com/ikerforce/spark-rita.git`.
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

El path default de amabri es hacia Blob Storage y no al hdfs local del clúster, por lo que en ambari hay que cambiar el valor de `fs.defaultFS` en la sección Advanced core-site por la opción `hdfs://mycluster/`.

### Descarga de datos

Una vez realizado el cambio de hdfs hay que seguir los siguientes pasos:

1. Crear una carpeta para almacenar información del ambiente: `hdfs dfs -mkdir /conda_envs/`.
2. Crear una carpeta para almacenar datos: `hdfs dfs -mkdir /samples/`.
3. Copiar el ambiente de anaconda a hdfs con el comando: `hdfs dfs -copyFromLocal conf/conda_envs/dask_yarn.tar.gz /conda_envs/`.
4. Copiar datos de blob storage a hdfs con el comando: `hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_dask_casted /samples/`.

### Cambiar la versión default de python a la 2.7 (comentar en .bashrc al final lo de anaconda)


hdfs dfs -mkdir /conda_envs/
hdfs dfs -mkdir /samples/
hdfs dfs -copyFromLocal conf/conda_envs/dask_yarn.tar.gz /conda_envs/
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_dask_casted /samples/
hdfs dfs -cp wasbs://transtat-tesis@ritatesisstorage.blob.core.windows.net/data_100K_spark_casted /samples/
hdfs dfs -mkdir hdfs://mycluster/spark/
hdfs dfs -mkdir hdfs://mycluster/spark/events/

Cambiar en Ambari las propiedades `Spark Eventlog directory` y `Spark History FS Log directory` por `hdfs://mycluster/spark/events/` en la sección Advanced spark2-defaults
Cambiar en Ambari las propiedades `Spark Eventlog directory` y `Spark History FS Log directory` por `hdfs://mycluster/spark/events/` en la sección Advanced spark2-thrift-sparkconf

curl ifconfig.me