from mpi_globals import RANK

debug = False

def set_debug(value: bool) -> None:
    global debug
    debug = value

def log(text: str) -> None:
    if debug:
        print(f"[{'Root' if RANK == 0 else f'Worker {RANK}'}] {text}")