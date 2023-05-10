import collections
import math
import sys
import os
import time

from typing import Callable, Generic, TypeVar
from dataclasses import dataclass
from mpi4py import MPI
from mpi_globals import CHANNEL, CLUSTER_SIZE, RANK
from message import ChunkAssignment, Finalize, WorkerDone

class WorkerResult:
    """Resultado del procesamiento de una parte de un archivo por parte de un Worker."""
    def as_bytes() -> bytes | bytearray:
        """Serializa el resultado y lo retorna como una secuencia de bytes."""
        pass

class BytesResult(WorkerResult):
    """Resultado del procesamiento de una parte de un archivo por parte de un Worker."""
    output: bytes | bytearray

    def __init__(self, output: bytes | bytearray) -> None:
        """Resultado del procesamiento de una parte de un archivo por parte de un Worker.

        Args:
            output (bytearray): El resultado en bytes de procesar una parte del archivo.
        """
        self.output = output

    def as_bytes(self) -> bytes | bytearray:
        return self.output

T = TypeVar("T", bound=WorkerResult)

@dataclass
class WorkLoad(Generic[T]):
    """Trabajo asignado a un Worker por parte de proceso raíz."""
    chunk: int
    result: T

class Process:
    """Proceso que existe junto con otros en un entorno de paralelismo."""
    current_chunk: int
    running: bool

    def __init__(self) -> None:
        """Proceso que existe junto con otros en un entorno de paralelismo."""
        self.current_chunk = 0
        self.running = False

    def broadcast(message: ChunkAssignment | WorkerDone | Finalize) -> None:
        """Realiza un broadcast del mensaje especificado y lo envía al resto de procesos.

        Args:
            message (ChunkAssignment | WorkerDone | Finalize): El mensaje a enviar.
        """
        for i in range(CLUSTER_SIZE):
            if i != RANK:
                CHANNEL.isend(message, i)

    def run(self) -> None:
        """Ejecuta al proceso."""
        self.running = True

        while self.running:
            self.process_loop()
            time.sleep(0.03)

    def process_loop(self) -> None:
        """Esto se va a ejecutar continuamente mientras el proceso esté activo."""
        pass

class Worker(Process, Generic[T]):
    """Proceso que existe junto con otros en un entorno de paralelismo. Se va a encargar de turnarse con otros para procesar un archivo por partes."""
    workload: WorkLoad[T] | None
    filename: str
    outfile: str
    chunk_processor: Callable[[str, int], T]
    write_callback: Callable[[str, int, T], None]

    def __init__(self, filename: str, outfile: str, chunk_processor: Callable[[str, int], T]) -> None:
        """Proceso que existe junto con otros en un entorno de paralelismo. Se va a encargar de turnarse con otros para procesar un archivo por partes.

        Args:
            filename (str): El archivo a procesar.
            outfile (str): El archivo donde se va a escribir el resultado de procesar el archivo de entrada.
            chunk_processor ((str, int) -> T): Función que le indica al worker como se va a procesar cada parte del archivo de entrada.
            Retorna el resultado que se va a escribir al archivo de salida.
        """
        super().__init__()
        self.filename = filename
        self.outfile = outfile
        self.workload = None
        self.chunk_processor = chunk_processor
        self.write_callback = lambda _, __, ___: None

    def before_write(self, write_callback: Callable[[str, int, T], None]) -> None:
        """Inscribe una función que se va a ejecutar justo antes de escribir al archivo de salida.
        Permite modificar lo que se va a escribir si esto depende de la salida del Worker anterior a este.

        Args:
            write_callback ((str, int, T) -> None): La función
        """
        self.write_callback = write_callback
    
    def process_loop(self) -> None:
        self.handle_messages()

        if self.workload and self.current_chunk == self.workload.chunk:
            self.write_callback(self.outfile, self.workload.chunk, self.workload.result)
            self.write_output()

    def handle_messages(self) -> None:
        """Recibe mensajes de otros procesos y actúa dependiendo del tipo de mensaje."""
        while CHANNEL.iprobe():
            message: ChunkAssignment | WorkerDone = CHANNEL.recv()

            match message:
                case ChunkAssignment(chunk_number):
                    result = self.chunk_processor(self.filename, chunk_number)
                    self.workload = WorkLoad(chunk_number, result)
                case WorkerDone(_):
                    self.current_chunk += 1
                case Finalize():
                    sys.exit(0)

    def write_output(self) -> None:
        """Escribe el resultado buffereado al archivo de salida."""
        with open(self.outfile, "ab") as out:
            out.write(self.workload.result.as_bytes())

        self.workload = None
        message = WorkerDone(RANK)
        Process.broadcast(message)
        self.current_chunk += 1

class Root(Process):
    """Proceso principal en un entorno de paralelismo. Está encargado de coordinar al resto de procesos y asignarles el trabajo que deben hacer."""
    free_workers: collections.deque[int]
    total_chunks: int
    outfile: str

    def __init__(self, filename: str, outfile: str, chunk_size: int) -> None:
        """Proceso principal en un entorno de paralelismo. Está encargado de coordinar al resto de procesos y asignarles el trabajo que deben hacer.

        Args:
            filename (str): El archivo a procesar.
            outfile (str): El archivo donde se va a escribir el resultado de procesar el archivo de entrada.
            chunk_size (int): Tamaño de cada parte del archivo.
        """
        super().__init__()
        self.free_workers = collections.deque(range(1, CLUSTER_SIZE))
        total_workload = os.stat(filename).st_size
        self.total_chunks = math.ceil(total_workload / chunk_size)
        self.outfile = outfile

        with open(outfile, "wb") as _:
            pass
    
    def process_loop(self) -> None:
        self.dispatch()
        self.handle_messages()
        self.handle_finalize()

    def dispatch(self) -> None:
        """Asigna trabajo a los Workers que estén disponibles."""
        while len(self.free_workers) > 0 and self.current_chunk < self.total_chunks:
            worker = self.free_workers.popleft()
            message = ChunkAssignment(self.current_chunk)
            CHANNEL.isend(message, worker)
            self.current_chunk += 1

    def handle_messages(self) -> None:
        """Recibe mensajes de los otros procesos y actúa dependiendo del tipo de mensaje."""
        while CHANNEL.iprobe():
            message: WorkerDone = CHANNEL.recv()
            self.free_workers.append(message.worker_rank)

    def handle_finalize(self) -> None:
        """Envía una señal de fin al resto de procesos y termina la ejecución del entorno de paralelismo."""
        if len(self.free_workers) == CLUSTER_SIZE - 1 and self.current_chunk == self.total_chunks:
            Process.broadcast(Finalize())
            self.running = False
            time.sleep(0.03)
            MPI.Finalize()