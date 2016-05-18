from collections import deque
from threading import Thread
from math import ceil
from threading import *
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch
from tide import TideService
from tide_connection import TideConnection


class ImportadorElastic:
    SQL_QUERY_COUNT = 'SELECT COUNT(1) FROM ocorrencia'
    SQL_QUERY_OCORRENCIA = 'SELECT o.* FROM ocorrencia o ORDER BY data_hora_criacao ASC LIMIT :limit OFFSET :offset'

    def __init__(self, numMaxThreads, nomeDoTipoDocumento, nomeDoIndice):
        self.fila = deque()
        self.numMaxThreads = numMaxThreads
        self.currentThreadGerenciadoraDeFila = None
        self.elastic = Elasticsearch()
        self.nomeDoTipoDocumento = nomeDoTipoDocumento
        self.nomeDoIndice = nomeDoIndice

    def iniciarServico(self):
        # self.currentThreadGerenciadoraDeFila = Thread(daemon=True, target=self.importarRegistros)
        self.importarRegistros()

    def importarRegistros(self):
        fetchSize = 100
        # totalOcorrencias = self.tideService.countOcorrencias()
        con = TideConnection().getConn()
        cur = con.cursor()

        cur.execute(self.SQL_QUERY_COUNT)
        totalOcorrencias = cur.fetchone()[0]

        with ThreadPoolExecutor(max_workers=self.numMaxThreads) as executor:
            total_paginas = (ceil(totalOcorrencias / fetchSize))
            for page in range(1, total_paginas):
                # constroi gerador de future inserindo ocorrencia
                con.commit()
                cur.execute(self.SQL_QUERY_OCORRENCIA.replace(':limit', str(fetchSize)).replace(':offset',
                                                                                                str(fetchSize * page)))
                ocorrencias = cur.fetchall()
                future_generator_ocorrencia = {executor.submit(self.__importar, o): o for o in ocorrencias}
                while (not as_completed(future_generator_ocorrencia)):
                    time.sleep(3)

    def __importar(self, registro):
        t = current_thread()
        try:
            # print("registro - %s \n" % (str(registro)))
            self.elastic.index(doc_type=self.nomeDoTipoDocumento, index=self.nomeDoIndice, body=registro)
        except Exception as e:
            print("Ocorreu um erro com a thread {}, no registro {} - {}\n".format(t, registro, e))


if __name__ == '__main__':
    imp = ImportadorElastic(20, 'tide', 'ocorrencia')
    imp.iniciarServico()
