from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import select

Base1 = declarative_base()


class Forma1(Base1):
    __tablename__ = "task_1"

    place = Column(Integer, primary_key=True)
    name = Column(String(40))
    points = Column(Integer)

    def __repr__(self):
        return f'{self.place:2}. place: {self.name:20} \t\t {self.points} pont'


with open('points.txt', 'r', encoding='utf8') as file_points:
    data_points = [int(row.strip()) for row in file_points]
with open('race4.txt', 'r', encoding='utf8') as race:
    data_names = [row.strip() for row in race]


engine = create_engine("mysql+pymysql://root@localhost/alchemy_test", echo=False, future=True)
Base1.metadata.create_all(engine)

with Session(engine) as session:
    for i, j in enumerate(data_names):
        if i < 10:
            my_data = Forma1(
                name = j,
                points = data_points[i]
            )
            session.add(my_data)
            session.commit()
        else:
            my_data = Forma1(
                name = j,
                points = 0
            )
            session.add(my_data)
            session.commit()

session = Session(engine)
query_1 = select(Forma1).where(Forma1.points > 0)

for data in session.scalars(query_1):
    print(data)

