from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

DSN = 'postgresql+asyncpg://app:secret@127.0.0.1:5431/app'
engine = create_async_engine(DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FIELDS = ['id', 'name', 'gender', 'height', 'mass', 'birth_year', 'skin_color', 'eye_color', 'hair_color', 'homeworld',
          'films', 'species', 'starships', 'vehicles']


class People(Base):
    __tablename__ = 'people_table'

    id = Column(Integer, primary_key=True)
    for field in FIELDS[1:]:
        vars()[field] = Column(String)
