__author__ = 'btoffoli'

from sqlalchemy import create_engine

class TideConnection:

    def __init__(self):
        self.__engineCurrent = None

    def getEngine(self):
        if not self.__engineCurrent:
            self.__engineCurrent = create_engine("postgresql://user:password@localhost/tide")
        return self.__engineCurrent



