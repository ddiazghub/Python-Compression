from dataclasses import dataclass

@dataclass
class WorkerDone:
    """Mensaje que indica que un Worker ha terminado el trabajo que se le ha asignado."""
    worker_rank: int

@dataclass
class ChunkAssignment:
    """Mensaje que indica que el proceso raiz le está asignando trabajo a un Worker."""
    chunk_number: int

@dataclass
class Finalize:
    """Mensaje que indica que todo el trabajo ha terminado."""
    pass