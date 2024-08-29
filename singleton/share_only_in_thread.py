import logging
import threading
import concurrent.futures
import time

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [Thread ID: %(thread)d] [Process: %(name)s] [%(funcName)s - %(lineno)d]: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ProcessFailure(Exception):
    pass

class LibrarySingleton:
    _instances = {}

    def __new__(cls, context_id= threading.get_ident()):
        if context_id not in cls._instances:
            cls._instances[context_id] = super(LibrarySingleton, cls).__new__(cls)
        return cls._instances[context_id]

    def __init__(self, context_id= threading.get_ident()):
        self.context_id = context_id
        self.data = None
    def get_instance():
        context_id= threading.get_ident()
        return LibrarySingleton._instances[context_id]
    
    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data

def outro_proc():
    return LibrarySingleton.get_instance().get_data()

class Orchestrator:
    def __init__(self, processes):
        self.processes = processes
        self.completed_processes = set()

    def execute_process(self, process_name):
        thread_id = threading.get_ident()  
        library_instance = LibrarySingleton(thread_id)
        library_instance.set_data(process_name)
        logging.info(f' Executando {process_name} : Teste Singleton {outro_proc()}')
        time.sleep(1)
        logging.info(f' Executado {process_name}  : Teste Singleton {outro_proc()}')
        if process_name == "fail":
            raise ProcessFailure(f"Processo {process_name} falhou!")
        return f"{process_name} executado com sucesso"

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            while self.processes:
                ready_to_run = [
                    process for process, dependencies in self.processes.items()
                    if not dependencies
                ]

                if not ready_to_run:
                    raise RuntimeError("Ciclo de dependências detectado ou dependências não resolvidas.")

                futures = {executor.submit(self.execute_process, process): process for process in ready_to_run}

                for future in concurrent.futures.as_completed(futures):
                    process = futures[future]
                    try:
                        result = future.result()
                        logger.info(result)
                        self.completed_processes.add(process)

                        del self.processes[process]
                        for deps in self.processes.values():
                            if process in deps:
                                deps.remove(process)
                    except ProcessFailure as e:
                        logger.info(e)
                        executor.shutdown(wait=False)
                        raise RuntimeError("Processo interrompido devido a falha.") from e

if __name__ == "__main__":
    processes = {
        "process1": [],
        "process2": ["process1"],
        "process3": ["process1"],
        "process4": ["process2", "process3"],
        # "fail": ["process1"],
        "process5": ["process4",]
    }

    orchestrator = Orchestrator(processes)

    try:
        orchestrator.run()
    except RuntimeError as e:
        logger.info(f"Erro: {e}")
