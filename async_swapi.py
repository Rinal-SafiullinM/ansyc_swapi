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


async def get_extra_fields(url_list, session):
    name_list = []
    for url in url_list:
        async with session.get(url) as response:
            json_res = await response.json()
            name_list.append(json_res['name'])
    return ', '.join(name_list)


async def get_films(url_list, session):
    name_list = []
    for url in url_list:
        async with session.get(url) as response:
            json_res = await response.json()
            name_list.append(json_res['title'])
    return ', '.join(name_list)


async def chain(person_id: int, session: ClientSession):
    """ Запускаем по цепочке функции (каждая последующая зависит от результата предыдущей) """
    person = await get_person(person_id)
    if person.get('detail', False):
        return {'id': person_id,
                'name': 'n/a',
                'birth_year': 'n/a',
                'gender': 'n/a',
                'height': 'n/a',
                'mass': 'n/a',
                'eye_color': 'n/a',
                'hair_color': 'n/a',
                'skin_color': 'n/a',
                'films': 'n/a',
                'homeworld': 'n/a',
                'species': 'n/a',
                'starships': 'n/a',
                'vehicles': 'n/a',
                }
    person['films'] = await get_films(person['films'], session)
    person['homeworld'] = await get_extra_fields(person['homeworld'], session)
    person['species'] = await get_extra_fields(person['species'], session)
    person['starships'] = await get_extra_fields(person['starships'], session)
    person['vehicles'] = await get_extra_fields(person['vehicles'], session)
    person_db = await get_fields(person_id, person)
    if not person_db:
        return 'Not found'
    await paste_person(person_db)


async def main(start: int, end: int):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async with ClientSession() as session:
        for id_chunk in chunked(range(start, end), CHUNK_SIZE):
            coroutines = [chain(i, session=session) for i in id_chunk]
            await asyncio.gather(*coroutines)


if __name__ == '__main__':
    start_time = time.time()
    asyncio.run(main(1, 85))
    print(f'TIME: {round(time.time() - start_time, 2)} s')
