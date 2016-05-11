from collections import deque
from threading import Thread
from math import ceil
from threading import *
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch
from tide import TideService
class ImportadorElastic:



    def __init__(self, numMaxThreads, nomeDoTipoDocumento, nomeDoIndice):
        self.fila = deque()
        self.numMaxThreads = numMaxThreads
        self.currentThreadGerenciadoraDeFila = None
        self.elastic = Elasticsearch()
        self.nomeDoTipoDocumento = nomeDoTipoDocumento
        self.nomeDoIndice = nomeDoIndice
        self.tideService = TideService()


    def iniciarServico(self):
        #self.currentThreadGerenciadoraDeFila = Thread(daemon=True, target=self.importarRegistros)
        self.importarRegistros()


    def importarRegistros(self):
        fetchSize = 100
        totalOcorrencias = self.tideService.countOcorrencias()

        with ThreadPoolExecutor(max_workers=self.numMaxThreads) as executor:
            for page in range(1..ceil(totalOcorrencias/fetchSize)):
                #constroi gerador de future inserindo ocorrencia
                ocorrencias = self.tideService.listOcorrencias(page * fetchSize)
                future_generator_ocorrencia = {executor.submit(self.__importar, o): o for o in ocorrencias}
                while (not as_completed(future_generator_ocorrencia)):
                    time.sleep(3)

    def __importar(self, registro):
        t = current_thread()
        try:
            self.elastic.index(doc_type=self.nomeDoTipoDocumento, index=self.nomeDoIndice, body=registro)
        except:
            print("Ocorreu um erro com a thread {}, no registro {}".format(t, registro))


if __name__ == '__main__':
    imp = ImportadorElastic(20, 'tide', 'ocorrencia')
    imp.iniciarServico()
