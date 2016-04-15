__author__ = 'bruno'

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from tide_connection import TideConnection

class TideService:

    def __init__(self):
        self.base = automap_base()
        self.tideConnection = TideConnection()
        self.base.prepare(self.tideConnection.getEngine(), reflect=True)
        #super().__init__()

    def __getClassOcorrencia(self):
            return self.base.classes.ocorrencia

    def __getSession(self):
        return Session(self.tideConnection.getEngine())


    def countOcorrencias(self):
        Ocorrencia = self.__getClassOcorrencia()
        query = self.__getSession().query(Ocorrencia)
        return query.count()


    def listOcorrencias(self, limite):
        q1 = self.__getSession().query(self.__getClassOcorrencia()).limit(limite)
        return q1.all()