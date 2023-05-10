import bisect
import sys

from argparse import ArgumentParser
from timeit import Timer
from typing import Callable
from constants import CHUNK_SIZE, REF_BYTE_LENGTH, WINDOW_SIZE
from mpi_globals import RANK
from process import BytesResult, Root, Worker
from descompresor import process_chunk
from reference import Reference

class DecompressionResult(BytesResult):
    """Resultado de la descompresión parcial de una parte de un archivo por parte de un Worker."""
    unresolved: list[tuple[int, Reference]]

    def __init__(self, output: bytearray, unresolved: list[tuple[int, Reference]]) -> None:
        """Resultado de la descompresión parcial de una parte de un archivo por parte de un Worker.
        
        Args:
            unresolved (list[(int, Reference)]): List de referencias LZ77 que no se pudieron descomprimir porque dependen de una parte anterior del archivo.
        """
        super().__init__(output)
        self.unresolved = unresolved

def process_chunk(chunk: bytes) -> DecompressionResult:
    """Descomprime parcialmente una parte de un archivo comprimido con el algoritmo LZ77.
    Se descomprime parcialmente porque en los casos en los cuales existen referencias a partes anteriores del archivo, no se puede extraer el valor descomprimido de estas referencias.
    Las referencias que no se puedan resolver se almacenan en una lista para ser resueltas justo antes de escribir al archivo de salida.
        
    Args:
        chunk (bytes): La parte del archivo comprimido.
    """
    output = bytearray()
    unresolved: list[tuple[int, Reference]] = []
    
    for i in range(0, len(chunk), REF_BYTE_LENGTH):
        ref = Reference.from_bytes(chunk[i: i + REF_BYTE_LENGTH])
        
        if ref.length > 0:
            out_len = len(output)
            unresolved_len = len(unresolved)
            match_start = out_len - ref.offset
            invalid = match_start < 0

            if not invalid and unresolved_len > 0:
                last_ref_index = bisect.bisect(unresolved, match_start, key=lambda x: x[0])
                last_ref_pos, last_ref = unresolved[last_ref_index - 1]
                next_ref_pos = sys.maxsize if last_ref_index == unresolved_len else unresolved[last_ref_index][0]
                invalid = last_ref_pos + last_ref.length >= match_start or next_ref_pos < match_start + ref.length
            
            if invalid:
                unresolved.append((out_len, ref))
                output.extend(b"\xff" * ref.length)
            else:
                output.extend(output[match_start: match_start + ref.length])
        
        output.append(ref.next_byte)

    return DecompressionResult(output, unresolved)

def chunk_processors(chunk_size: int) -> tuple[Callable[[str, int], DecompressionResult], Callable[[str, int, DecompressionResult], None]]:
    """Funciones que permiten descomprimir un archivo comprimido con el algoritmo LZ77.
    La primera descomprime parcialmente una parte del archivo. Retorna a la parte descomprimida como un buffer de bytes y la lista de referencias que no pudieron ser resueltas.
    La segunda se ejecuta justo antes de escribir al archivo de salida y lee la anterior parte descomprimida para resolver referencias.
        
    Args:
        chunk_size (int): Tamaño de cada parte del archivo.
    """
    def decompress_chunk(filename: str, chunk_number: int) -> DecompressionResult:
        """Lee y descomprime parcialmente una parte de un archivo comprimido con el algoritmo LZ77.
            
        Args:
            filename (str): Nombre del archivo comprimido.
            chunk_number (int): El número de la parte a descomprimir.
        """
        chunk_start = chunk_number * chunk_size

        with open(filename, "rb") as file:
            file.seek(chunk_start)
            chunk = file.read(chunk_size)

            return process_chunk(chunk)
 
    def resolve_references(outfile: str, chunk_number: int, result: DecompressionResult) -> None:
        """Función que descomprime todas las referencias a datos anteriores faltantes de un archivo comprimido con el algoritmo LZ77.
        
        Args:
            filename (str): Nombre del archivo comprimido.
            chunk_number (int): El número de la parte a descomprimir.
        """
        if chunk_number > 0:
            with open(outfile, "rb") as file:
                file.seek(-WINDOW_SIZE, 2)
                window = file.read()
                buffer = bytearray(window + result.output)

                for i, ref in result.unresolved:
                    position = len(window) + i - ref.offset
                    result.output[i: i + ref.length] = buffer[position: position + ref.length]
                    buffer[len(window) + i: len(window) + i + ref.length] = buffer[position: position + ref.length]

    return decompress_chunk, resolve_references

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Descompresor LZ77 en paralelo",
        description="Descomprime un archivo comprimido usando el algoritmo LZ77 en paralelo"
    )

    parser.add_argument("zipfile", help="Nombre del archivo a descomprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo descomprimido", default="descomprimidop-elmejorprofesor.txt")
    parser.add_argument("-c", "--chunk-size", help="Tamaño de las partes en las cuales se dividirá el archivo de entrada", type=int, default=CHUNK_SIZE)

    args = parser.parse_args()
    zipfile, outfile, chunk_size = args.zipfile, args.outfile, args.chunk_size
    chunk_size = chunk_size + REF_BYTE_LENGTH - chunk_size % REF_BYTE_LENGTH

    if RANK == 0:
        root_process = Root(zipfile, outfile, chunk_size)
        timer = Timer(lambda: root_process.run())
        print(timer.timeit(1))
    else:
        dec_chunk, resolve_refs = chunk_processors(chunk_size)
        worker = Worker(zipfile, outfile, dec_chunk)
        worker.before_write(resolve_refs)
        worker.run()