__author__ = 'bruno'

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from tide_connection import TideConnection

Base = automap_base()

# engine, suppose it has two tables 'user' and 'address' set up
# engine = create_engine("sqlite:///mydatabase.db")
engine = TideConnection().getEngine()


# reflect the tables
Base.prepare(engine, reflect=True)

#print(Base.classes)

Ocorrencia  = Base.classes.ocorrencia

#Localizacao = Base.classes.localizacao

o =  Ocorrencia()

#l = Localizacao()

session = Session(engine)

q1 = session.query(Ocorrencia).limit(10)

l = q1.all()

print(l)

print(dir(o))
# mapped classes are now created with names by default
# matching that of the table name.
# User =  Base.classes.user
# Address = Base.classes.address
#
# session = Session(engine)
#
# # rudimentary relationships are produced
# session.add(Address(email_address="foo@bar.com", user=User(name="foo")))
# session.commit()
#
# # collection-based relationships are by default named
# # "<classname>_collection"
# print (u1.address_collection)
