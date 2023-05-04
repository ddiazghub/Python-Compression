from mpi4py import MPI

CHANNEL = MPI.COMM_WORLD
RANK = CHANNEL.Get_rank()
CLUSTER_SIZE = CHANNEL.Get_size()