import time
import asyncio
from aiohttp import ClientSession
from more_itertools import chunked
from db import engine, Session, People, Base, FIELDS

URL = 'https://swapi.dev/api/people/'
CHUNK_SIZE = 15


async def get_person(person_id: int):
    """ Получаем данные по запросу """
    print(f'{person_id:>2} - start request GET')
    session = ClientSession()
    response = await session.get(f'{URL}{person_id}')
    person = await response.json()
    print(f'__{person_id:>2} - response.json')
    await session.close()
    return person


async def get_fields(person_id: int, person: dict):
    """ Формируем поля для базы данных """
    if len(person) == 1:
        print(f'<person (id={person_id:>2}) not found>')
        return None
    person_db = {}
    for field, value in person.items():
        if value and field in FIELDS:
            if isinstance(value, list):
                person_db[field] = ', '.join(value)
            else:
                person_db[field] = value
    person_db['id'] = person_id
    print(f'___{person_id:>2} - fields for the database are ready')
    return person_db


async def paste_person(person_db: dict):
    """ Вставляем данные в базу данных """
    async with Session() as session:
        people_orm = People(**person_db)
        session.add(people_orm)
        await session.commit()
        print(f'____{person_db["id"]:>2} - added to the database')


async def chain(person_id: int):
    """ Запускаем по цепочке функции (каждая последующая зависит от результата предыдущей) """
    person = await get_person(person_id)
    person_db = await get_fields(person_id, person)
    if not person_db:
        return 'Not found'
    await paste_person(person_db)


async def main(start: int, end: int):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    for id_chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines = [chain(i) for i in id_chunk]
        await asyncio.gather(*coroutines)


if __name__ == '__main__':
    start_time = time.time()
    asyncio.run(main(1, 85))
    print(f'TIME: {round(time.time() - start_time, 2)} s')
