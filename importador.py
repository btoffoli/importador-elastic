from collections import deque
from threading import Thread
from math import ceil
from threading import *
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch
from tide import TideService
from tide_connection import TideConnection
from sys import argv


class ImportadorElastic:
    @staticmethod
    def ocorrencia_to_map(o, pagina):
        """
            o.id,
            o.protocolo,
            o.data_hora_criacao,
            o.data_hora_fim,
            (o.data_hora_fim - o.data_hora_inicio) AS tempo_total,
            avg(emp.data_hora_fim - emp.data_hora_criacao) AS tempo_empenho_total,
            count(emp.id) as qtd_empenhos,
            ST_X(loc.poligono) as longitude,
            ST_X(loc.poligono) as latitude
        :param o:

        :return:
        """
        map = {
            'pagina': pagina,
            'id': o[0],
            'protocolo': o[1],
            'data_hora_criacao': o[2],
            'data_hora_fim': o[3],
            'tempo_total': int(o[4].total_seconds()),
            'tempo_empenho_total': int(o[5].total_seconds()),
            'qtd_empenhos': o[6],
            'longitude': o[7],
            'latitude': o[8]

        }
        return map

    URL_ELASTIC = 'http://192.168.0.7:9200'
    SQL_QUERY_COUNT = 'SELECT COUNT(1) FROM ocorrencia'
    SQL_QUERY_OCORRENCIA = '''
    SELECT
        o.id,
        o.protocolo,
        o.data_hora_criacao,
        o.data_hora_fim,
        (o.data_hora_fim - o.data_hora_inicio) AS tempo_total,
        avg(emp.data_hora_fim - emp.data_hora_criacao) AS tempo_empenho_empenho,
        count(emp.id) as qtd_empenhos,
        ST_X(loc.poligono) as longitude,
        ST_Y(loc.poligono) as latitude
    FROM ocorrencia o
    JOIN localizacao loc
    ON loc.ocorrencia_id = o.id
    LEFT JOIN empenho emp
    ON emp.ocorrencia_id = o.id
    WHERE o.data_hora_fim IS NOT NULL
    AND emp.data_hora_criacao IS NOT NULL
    GROUP BY o.id, o.protocolo, ST_X(loc.poligono),
    ST_Y(loc.poligono), loc.data_hora_criacao, loc.ocorrencia_id
    HAVING MAX(loc.data_hora_criacao) = loc.data_hora_criacao
    ORDER BY o.data_hora_criacao ASC
    LIMIT :limit
    OFFSET :offset
    '''

    def __init__(self, numMaxThreads, nomeDoTipoDocumento, nomeDoIndice, urlDBTide = None, urlElastic = None):
        self.fila = deque()
        self.numMaxThreads = numMaxThreads
        self.currentThreadGerenciadoraDeFila = None
        if urlElastic:
            self.elastic = Elasticsearch([urlElastic])
        else:
            self.elastic = Elasticsearch([self.URL_ELASTIC])
        self.nomeDoTipoDocumento = nomeDoTipoDocumento
        self.nomeDoIndice = nomeDoIndice
        self.urlDBTide = urlDBTide

    def iniciarServico(self):
        # self.currentThreadGerenciadoraDeFila = Thread(daemon=True, target=self.importarRegistros)
        self.importarRegistros()

    def importarRegistros(self):
        fetchSize = 100
        # totalOcorrencias = self.tideService.countOcorrencias()
        con = None
        if self.urlDBTide:
            con = TideConnection().getConn(self.urlDBTide)
        else:
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
                future_generator_ocorrencia = {executor.submit(self.__importar, ImportadorElastic.ocorrencia_to_map(o, page)): o for o in ocorrencias}
                while (not as_completed(future_generator_ocorrencia)):
                    time.sleep(3)

    def __importar(self, registro, vez = 0):
        t = current_thread()
        try:
            # print("registro - %s \n" % (str(registro)))
            self.elastic.index(doc_type=self.nomeDoTipoDocumento, index=self.nomeDoIndice, id=int(registro['id']), body=registro)
        except TimeoutError as timeoutEx:
            if vez < 3:
                self.__importar(registro, vez + 1)
            else:
                print("3 tentativas falharam ao tentar inserir ocorrencia %s" % registro)
        except Exception as e:
            print("Ocorreu um erro com a thread {}, no registro {} - {}\n".format(t, registro, e))
            #Tenta inserir novamente



if __name__ == '__main__':
    urlDBTide = argv[1]
    urlElastic = argv[2]
    imp = ImportadorElastic(30, 'tide', 'ocorrencia', urlDBTide, urlElastic)
    imp.iniciarServico()
