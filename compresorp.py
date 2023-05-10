from argparse import ArgumentParser
from timeit import Timer
from typing import Callable
from constants import CHUNK_SIZE, WINDOW_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from compresor import process_chunk

def chunk_processor(chunk_size: int) -> Callable[[str, int], BytesResult]:
    """
    Retorna una función que permite comprimir una parte del tamaño especificado de un archivo de texto.

    Args:
        chunk_size (int): El tamaño de cada parte del archivo.
    """
    def compress_chunk(filename: str, chunk_number: int) -> BytesResult:
        """
        Lee una parte de un archivo de texto y la comprime.

        Args:
            filename (str): El nombre del archivo de texto.
            chunk_number (int): El número de la parte a comprimir.
        """
        chunk_start = chunk_number * chunk_size
        
        with open(filename, "rb") as file:
            windows_start = max(chunk_start - WINDOW_SIZE, 0)
            file.seek(windows_start)
            window = b"" if chunk_number == 0 else file.read(WINDOW_SIZE)
            window_size = len(window)
            chunk = file.read(chunk_size)
            output = process_chunk(window + chunk, window_size)
            
            return BytesResult(output)
    
    return compress_chunk


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor LZ77 en paralelo",
        description="Comprime un archivo usando el algoritmo LZ77 en paralelo"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimidop.elmejorprofesor")
    parser.add_argument("-c", "--chunk-size", help="Tamaño de las partes en las cuales se dividirá el archivo de entrada", type=int, default=CHUNK_SIZE)

    args = parser.parse_args()
    filename, outfile, chunk_size = args.filename, args.outfile, args.chunk_size

    if RANK == 0:
        root_process = Root(filename, outfile, chunk_size)
        timer = Timer(lambda: root_process.run())
        print(timer.timeit(1))
    else:
        worker = Worker(filename, outfile, chunk_processor(chunk_size))
        worker.run()