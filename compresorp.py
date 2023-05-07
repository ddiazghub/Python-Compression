from argparse import ArgumentParser
from typing import Callable
from constants import CHUNK_SIZE, WINDOW_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from logger import log, set_debug
from compresor import process_chunk, window_match


def chunk_processor(chunk_size: int) -> Callable[[str, int], BytesResult]:
    def compress_chunk(filename: str, chunk_number: int) -> BytesResult:
        chunk_start = chunk_number * chunk_size
        output = bytearray()

        
        with open(filename, "rb") as file:
            windows_start=max(chunk_start-chunk_size,0)
            file.seek(windows_start)
            window = b"" if chunk_start == 0 else file.read(WINDOW_SIZE)
            window_size = WINDOW_SIZE if chunk_number > 0 else 0
            chunk = file.read(chunk_size)
            o1 = process_chunk(window+chunk, window_size)
            output.extend(o1)

        return BytesResult(output)
    
    return compress_chunk


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor LZ77",
        description="Comprime un archivo usando el algoritmo LZ77"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")
    parser.add_argument("-d", "--debug", help="Imprime logs en consola acerca del estado de la aplicación", action="store_true")
    parser.add_argument("-c", "--chunk-size", help="Tamaño de las partes en las cuales se dividirá el archivo de entrada", type=int, default=CHUNK_SIZE)

    args = parser.parse_args()
    filename, outfile, chunk_size = args.filename, args.outfile, args.chunk_size
    set_debug(args.debug)
    
    #timer = Timer(lambda: compress(filename, outfile))
    #(f"\nTiempo de ejecución: {timer.timeit(1)} segundos")

    if RANK == 0:
        root_process = Root(filename, outfile, chunk_size)
        root_process.run()
    else:
        worker = Worker(filename, outfile, chunk_processor(chunk_size))
        worker.run()