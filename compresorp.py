from argparse import ArgumentParser
from timeit import Timer
from typing import Callable
from constants import CHUNK_SIZE, WINDOW_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from compresor import process_chunk


def chunk_processor(chunk_size: int) -> Callable[[str, int], BytesResult]:
    def compress_chunk(filename: str, chunk_number: int) -> BytesResult:
        chunk_start = chunk_number * chunk_size
        
        with open(filename, "rb") as file:
            windows_start=max(chunk_start - WINDOW_SIZE, 0)
            file.seek(windows_start)
            window = b"" if chunk_start == 0 else file.read(WINDOW_SIZE)
            window_size = WINDOW_SIZE if chunk_number > 0 else 0
            chunk = file.read(chunk_size)
            output = process_chunk(window+chunk, window_size)
            
            return BytesResult(output)
    
    return compress_chunk


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor LZ77",
        description="Comprime un archivo usando el algoritmo LZ77"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")
    parser.add_argument("-c", "--chunk-size", help="Tamaño de las partes en las cuales se dividirá el archivo de entrada", type=int, default=CHUNK_SIZE)

    args = parser.parse_args()
    filename, outfile, chunk_size = args.filename, args.outfile, args.chunk_size
    
    #timer = Timer(lambda: compress(filename, outfile))
    #(f"\nTiempo de ejecución: {timer.timeit(1)} segundos")

    if RANK == 0:
        root_process = Root(filename, outfile, chunk_size)
        timer = Timer(lambda: root_process.run())
        print(timer.timeit(1))
    else:
        worker = Worker(filename, outfile, chunk_processor(chunk_size))
        worker.run()