import collections
import math
import sys
import time
import os

from typing import Callable, Generic, TypeVar
from dataclasses import dataclass
from mpi4py import MPI
from mpi_globals import CHANNEL, CLUSTER_SIZE, RANK
from message import ChunkAssignment, Finalize, WorkerDone
from logger import log

class WorkerResult:
    def as_bytes() -> bytes | bytearray:
        pass

class BytesResult(WorkerResult):
    output: bytearray

    def __init__(self, output: bytearray) -> None:
        self.output = output

    def as_bytes(self) -> bytearray:
        return self.output

T = TypeVar("T", bound=WorkerResult)

@dataclass
class WorkLoad(Generic[T]):
    chunk: int
    result: T

class Process:
    current_chunk: int

    def __init__(self) -> None:
        self.current_chunk = 0

    def broadcast(message: ChunkAssignment | WorkerDone | Finalize) -> None:
        for i in range(CLUSTER_SIZE):
            if i != RANK:
                CHANNEL.isend(message, i)

class Worker(Process, Generic[T]):
    workload: WorkLoad[T] | None
    filename: str
    outfile: str
    chunk_processor: Callable[[str, int], T]
    write_callback: Callable[[T], None]

    def __init__(self, filename: str, outfile: str, chunk_processor: Callable[[str, int], T]) -> None:
        super().__init__()
        self.filename = filename
        self.outfile = outfile
        self.workload = None
        self.chunk_processor = chunk_processor
        self.write_callback = lambda _: None

    def before_write(self, write_callback: Callable[[T], None]) -> None:
        self.write_callback = write_callback
    
    def run(self) -> None:
        log(f"Process started")

        while True:
            self.handle_messages()

            if self.workload and self.current_chunk == self.workload.chunk:
                self.write_callback(self.workload.result)
                self.write_output()

    def handle_messages(self) -> None:
        while CHANNEL.iprobe():
            message: ChunkAssignment | WorkerDone = CHANNEL.recv()

            match message:
                case ChunkAssignment(chunk_number):
                    log(f"Received workload assignment for chunk {chunk_number}")
                    result = self.chunk_processor(self.filename, chunk_number)
                    self.workload = WorkLoad(chunk_number, result)
                    log(f"Finished processing chunk. Waiting to write output.")
                case WorkerDone(worker_rank):
                    self.current_chunk += 1
                    log(f"Worker {worker_rank} is done with it's workload. Current chunk is {self.current_chunk}")
                case Finalize():
                    log("All work has finished. Exiting.")
                    sys.exit(0)

    def write_output(self) -> None:
        with open(self.outfile, "ab") as out:
            out.write(self.workload.result.as_bytes())

        self.workload = None
        message = WorkerDone(RANK)
        
        log(f"Writing to output.")
        
        Process.broadcast(message)
        self.current_chunk += 1

class Root(Process):
    free_workers: collections.deque[int]
    total_chunks: int
    outfile: str

    def __init__(self, filename: str, outfile: str, chunk_size: int) -> None:
        super().__init__()
        self.free_workers = collections.deque(range(1, CLUSTER_SIZE))
        total_workload = os.stat(filename).st_size
        self.total_chunks = math.ceil(total_workload / chunk_size)
        self.outfile = outfile

        with open(outfile, "wb") as _:
            pass
    
    def run(self) -> None:
        log(f"Process started. Total chunks: {self.total_chunks}")

        while True:
            self.dispatch()
            self.handle_messages()
            self.handle_finalize()

    def dispatch(self) -> None:
        while len(self.free_workers) > 0 and self.current_chunk < self.total_chunks:
            worker = self.free_workers.popleft()
            log(f"Dispatching workload to worker with rank {worker}. Current chunk index: {self.current_chunk}")
            message = ChunkAssignment(self.current_chunk)
            CHANNEL.isend(message, worker)
            self.current_chunk += 1

    def handle_messages(self) -> None:
        while CHANNEL.iprobe():
            message: WorkerDone = CHANNEL.recv()
            self.free_workers.append(message.worker_rank)
            log(f"Worker with rank {message.worker_rank} has finished it's work. Registering as free.")
            log(f"{len(self.free_workers)} workers are currently free.")

    def handle_finalize(self) -> None:
        if len(self.free_workers) == CLUSTER_SIZE - 1 and self.current_chunk == self.total_chunks:
            Process.broadcast(Finalize())
            time.sleep(0.5)
            MPI.Finalize()
            log("All work has finished. Exiting.")
            log(f"Check file {self.outfile}. To see output.")
            sys.exit(0)