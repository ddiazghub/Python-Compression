from argparse import ArgumentParser
from constants import CHUNK_SIZE
from mpi_globals import RANK
from process import Root, Worker
from logger import set_debug

def process_chunk(filename: str, chunk_number: int) -> bytearray:
    chunk_start = chunk_number * CHUNK_SIZE
    chunk_end = chunk_start + CHUNK_SIZE
    output = bytearray()

    for i in range(chunk_start, chunk_end, 1):
       output.extend(f"{i}\n".encode("utf-8"))

    return output

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor LZ77",
        description="Comprime un archivo usando el algoritmo LZ77"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")
    parser.add_argument("-d", "--debug", help="Imprime logs en consola acerca del estado de la aplicación", action="store_true")

    args = parser.parse_args()
    filename, outfile = args.filename, args.outfile
    set_debug(args.debug)
    
    #timer = Timer(lambda: compress(filename, outfile))
    #(f"\nTiempo de ejecución: {timer.timeit(1)} segundos")

    if RANK == 0:
        root_process = Root(filename, outfile, CHUNK_SIZE)
        root_process.run()
    else:
        worker = Worker(filename, outfile, process_chunk)
        worker.run()