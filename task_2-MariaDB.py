from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session


Base = declarative_base()


class Forma1(Base):
    __tablename__ = 'task_2'

    id = Column(Integer, primary_key=True)
    place = Column(Integer)
    name = Column(String(40))
    race = Column(Integer)

    def __repr__(self):
        return f'{self.place}. place - Name: {self.name}'


engine = create_engine("mysql+pymysql://root@localhost/alchemy_test", echo=False, future=True)
Base.metadata.create_all(engine)

with Session(engine) as session:
    for i in range(4):
        with open(f'race{i+1}.txt', 'r', encoding='utf8') as races:
            for j in range(3):
                kell = races.readline()
                my_data_db = Forma1(
                    place = j+1,
                    name = kell.strip(),
                    race = i+1
                )
                session.add(my_data_db)
                session.commit()

session = Session(engine)
query_1 = select(Forma1).where(Forma1.race == 1)
query_2 = select(Forma1).where(Forma1.race == 2)
query_3 = select(Forma1).where(Forma1.race == 3)
query_4 = select(Forma1).where(Forma1.race == 4)
# res = db.select([db.func.count()]).select_from(mar13).scalar()
place_1 = select(Forma1).where(Forma1.place == 1).group_by(Forma1.name)

print('The 1.run first 3 places:')
for i in session.scalars(query_1):
    print(i)

print('The 2.run first 3 places:')
for i in session.scalars(query_2):
    print(i)

print('The 3.run first 3 places:')
for i in session.scalars(query_3):
    print(i)

print('The 4.run first 3 places:')
for i in session.scalars(query_4):
    print(i)

print("\nWhos reached first place in the last 4 run:")
for i in session.scalars(place_1):
    print(i)

all_player = select(Forma1.name).group_by(Forma1.name)
print("\nOnly that racers reached any rank from top 3: ")
for i in session.scalars(all_player):
    print(i)
