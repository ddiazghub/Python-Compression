from argparse import ArgumentParser
from timeit import Timer
from typing import Callable
from constants import CHUNK_SIZE, REF_BYTE_LENGTH, WINDOW_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from descompresor import process_chunk
from reference import Reference
import bisect

class DecompressionResults(BytesResult):
    unresolved: list[tuple[int, Reference]]

    def __init__(self, output: bytearray, unresolved) -> None:
        super().__init__(output)
        self.unresolved = unresolved

def process_chunk(chunk: bytes, chunk_start: int) -> bytearray:
    output = bytearray()
    unresolved = []
    
    for i in range(0, len(chunk), REF_BYTE_LENGTH):
        ref = Reference.from_bytes(chunk[i: i + REF_BYTE_LENGTH])
        match_start = len(output) - ref.offset
        ref_byte = output[match_start: match_start + ref.length]
        if match_start < 0 or ref_byte == b"\xff" * ref.length:
            unresolved.append((chunk_start + i, ref))
        output.extend(ref_byte)
        output.append(ref.next_byte)

    return DecompressionResults(output, unresolved)

def chunk_processor(chunk_size: int) -> Callable[[str, int], DecompressionResults]:
    def decompress_chunk(filename: str, chunk_number: int) -> DecompressionResults:
        chunk_start = chunk_number * chunk_size
        
        with open(filename, "rb") as file:
            chunk = file.read(chunk_size)

            return process_chunk(chunk, chunk_start)
 
    return decompress_chunk

def resolve_references(result: DecompressionResults) -> None:
    pass 

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
        worker.before_write(resolve_references)
        worker.run()