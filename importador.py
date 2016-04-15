from collections import deque
from threading import Thread
from math import ceil
from threading import *
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from tide import TideService
class ImportadorElastic:



    def __init__(self, numMaxThreads, nomeDoTipoDocumento, nomeDoIndice):
        self.fila = deque()
        self.executor = ThreadPoolExecutor(max_workers=numMaxThreads)
        self.currentThreadGerenciadoraDeFila = None
        self.elastic = Elasticsearch()
        self.nomeDoTipoDocumento = nomeDoTipoDocumento
        self.nomeDoIndice = nomeDoIndice
        self.tideService = TideService()



    def iniciarServico(self):
        self.currentThreadGerenciadoraDeFila = Thread(daemon=True, target=)

    def importarRegistros(self):
        fetchSize = 100
        totalOcorrencias = self.tideService.countOcorrencias()
        for page in range(1..ceil(totalOcorrencias/fetchSize)):
            ocorrencias = self.tideService.listOcorrencias(page*fetchSize)
            for o in ocorrencias:
                self.fila.append(o)


        return self.executor.submit(self.__importar, registro)

    def __importar(self, registro):
        t = current_thread()
        try:
            self.elastic.index(doc_type=self.nomeDoTipoDocumento, index=self.nomeDoIndice, body=registro)
        except:
            print("Ocorreu um erro com a thread {}, no registro {}".format(t, registro))
