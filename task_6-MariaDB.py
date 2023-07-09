from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()


class Racers(Base):
    __tablename__ = "task_6"

    id = Column(Integer, primary_key=True)
    second_name = Column(String(20))
    first_name = Column(String(20))
    country = Column(String(20))
    start_number = Column(Integer)
    team = Column(String(40))

    def __repr__(self):
        return f'{self.start_number:7}   {self.second_name:10} {self.first_name:16} {self.country}'


engine = create_engine("mysql+pymysql://root@localhost/alchemy_test")
Base.metadata.create_all(engine)

with Session(engine) as session:
    with open('racers.txt', 'r', encoding='utf8') as file:
        for row in file:
            my_data = row.strip().split("\t")
            my_db = Racers(
                second_name = my_data[0],
                first_name = my_data[1],
                country = my_data[2],
                start_number = my_data[3],
                team = my_data[4]
            )
            session.add(my_db)
            session.commit()

session = Session(engine)

print(f'{"Start p.":13}{"Racers name":25}{"Country":50}\n{"-"*80}')
for data_1 in session.scalars(select(Racers)):
    print(data_1)

print("-"*80)
q_2 = session.query(Racers).where(Racers.start_number > 0)
for data_2 in q_2:
    print(f'{data_2.start_number:7}   {data_2.second_name:10} {data_2.first_name:16}')

print("-"*80)
print('Which Countries gave racers, and how many: ')
q_3 = session.query(Racers.country).group_by(Racers.country).add_columns(func.count(Racers.country))
for j in q_3:
    print(f'{j[0]:20} {j[1]}')

print("-"*80)
print('How many racers have a team: ')
q_4 = session.query(Racers.team).group_by(Racers.team).add_columns(func.count(Racers.team))
for j in q_4:
    print(f'{j[0]:40} {j[1]}')

