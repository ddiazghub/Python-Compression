from dataclasses import dataclass

@dataclass
class WorkerDone:
    worker_rank: int

@dataclass
class ChunkAssignment:
    chunk_number: int

@dataclass
class Finalize:
    pass