import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import numpy as np
import time

t_inicial = time.time()
t_ant = t_inicial

if __name__ == '__main__':
	client = Client()

	client

	max_number_of_nodes = 3500
	number_of_ties = 1_000
	network = pd.DataFrame(np.random.randint(max_number_of_nodes, size=(number_of_ties,2)), columns=['source', 'target'])
	ddf = dd.from_pandas(network, npartitions=10)

	for i in range(100):
	    ddf = ddf[ddf['source']//i!=5]
	    ddf = client.persist(ddf)
	    t_actual = time.time() - t_ant
	    t_ant = time.time()
	    print(t_actual)