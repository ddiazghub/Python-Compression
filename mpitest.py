from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
rt = []

if rank == 0:
    for i in range(size):
        comm.send(i, dest=i)
    
    for i in range(size):
        dt = comm.recv(source=i)
        rt.append(dt)
    
    print(rt)
else:
    data = comm.recv(source=0)
    comm.send(data*data, dest=0)