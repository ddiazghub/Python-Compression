from argparse import ArgumentParser
from typing import Callable
from constants import CHUNK_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from logger import log, set_debug

class CountResult(BytesResult):
    chunk_number: int

    def __init__(self, output: bytearray, chunk_number: int) -> None:
        super().__init__(output)
        self.chunk_number = chunk_number

def chunk_processor(chunk_size: int) -> Callable[[str, int], CountResult]:
    def process_chunk(filename: str, chunk_number: int) -> CountResult:
        chunk_start = chunk_number * chunk_size
        chunk_end = chunk_start + chunk_size
        output = bytearray()

        for i in range(chunk_start, chunk_end, 1):
            output.extend(f"{i}\n".encode("utf-8"))

        return CountResult(output, chunk_number)
    
    return process_chunk

def print_chunk_number(result: CountResult) -> None:
    log(f"Writing chunk {result.chunk_number} to output file.")

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
        worker.before_write(print_chunk_number)
        worker.run()