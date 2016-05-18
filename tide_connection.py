__author__ = 'btoffoli'

from sqlalchemy import create_engine
from psycopg2 import connect as connection

class TideConnection:

    #def __init__(self):
    #    self.__engineCurrent = None

    #def getEngine(self):
    #    if not self.__engineCurrent:
    #        self.__engineCurrent = create_engine("postgresql://geocontrol:geo007@localhost/tide")
    #    return self.__engineCurrent

    def __init__(self):
        self.__conn = None

    def getConn(self):
        return connection("dbname='tide' user='geocontrol' host='localhost' password='geo007'")

