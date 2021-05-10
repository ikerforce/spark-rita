yum -y install wget
yum -y install git
sudo echo 'export ARROW_LIBHDFS_DIR="/usr/hdp/4.1.4.0"' >> ~/.bashrc
sudo echo 'export PATH="$ARROW_LIBHDFS_DIR:$PATH"' >> ~/.bashrc
source ~/.bashrc
git clone https://ikerforce:$1@github.com/ikerforce/spark-rita.git
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
eval "$($HOME/miniconda/bin/conda shell.bash hook)"
conda init
conda config --set auto_activate_base false
conda deactivate
cd spark-rita
conda env create -f conf/conda_envs/dask_yarn.yml
conda activate dask_yarn
dask-scheduler