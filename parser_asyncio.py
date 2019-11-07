import asyncio
from aiohttp import ClientSession

from time import sleep
from lxml import html

import pandas as pd
import random


vacancy_proc_count = 0
res = []

def get_content(page_content, url):
    # Получаем корневой lxml элемент из html страницы.
    document = html.fromstring(page_content)

    def get(xpath):
        item = document.xpath(xpath)
        if item:
            return item[-1]
        return None


    name = get('/html/body/div[1]/div/div[1]/div[2]/div/h2//text()')

    tag = get('/html/body/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/span/span[1]//text()')
    tag_i = 1
    tags = [tag]
    while tag != None:
        tag_i += 1
        tag = get('/html/body/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/span/span[{}]//text()'.format(tag_i))
        tags.append(tag)
    del tags[-1]


    reqs = []
    for ul in range(5):
        for li in range(5):
            req = get('/html/body/div[1]/div/div[2]/div[1]/div[1]/div[3]/ul[{}]/li[{}]//text()'.format(ul, li))
            if req != None:
                reqs.append(req)

    answer = {'url': url, 'name': name, 'tags': tags, 'requirement': reqs}
    return answer


async def get_one_task(url, session):
    global vacancy_proc_count
    async with session.get(url) as response:
        # Ожидаем ответа, получаем контент
        page_content = await response.read()

        # Парсим содержимое контента
        item = get_content(page_content, url)

        res.append(item)
        vacancy_proc_count += 1
        print('Стр.: ' + str(vacancy_proc_count) + ' Запарсено: ' + url)


async def bound_screen(smp, url, session):
    try:
        async with smp:
            await get_one_task(url, session)
    except Exception:
        print('Exception:\n', Exception)
        # Блокируем запросы на 10с если ошибка 429 (много запросов)
        sleep(10)


async def run(urls):
    tasks = []
    # Ограничение для запуска не более 20 асинхронных процессов
    smp = asyncio.Semaphore(3)
    headers = {'User-Agent': 'Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101'}

    # Запускаем 20 процессов
    async with ClientSession(headers=headers) as session:
        # Передаем по урлу в один процесс
        for url in urls:
            task = asyncio.ensure_future(bound_screen(smp, url, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


def main():
    global res

    while True:
        gen_read = input('Генерировать 100 urls (g) или читать из файла (r)? - g/r: ')

        if gen_read == 'g':
            prev_url = 'https://team.mail.ru/vacancy/'
            urls = [prev_url + str(random.randint(8000, 10000)) + '/' for i in range(100)]
            break
        if gen_read == '':
            urls = [line[:-1] for line in open('urls_vacancy.txt', 'r')]
            break


    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls))
    loop.run_until_complete(future)


    res = pd.DataFrame(res).dropna(axis=0).reset_index(drop=True)
    print(res.head())
    res.to_csv('vacancy_mail_group.csv', sep=',')


if __name__ == '__main__':
    main()
