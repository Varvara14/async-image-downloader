import asyncio
import os
import aiofiles
import aiohttp
from contextlib import suppress
from aioconsole import ainput
from prettytable import PrettyTable

async def is_path_writable(path):
    try:
        test_file = os.path.join(path, '.write_test')
        async with aiofiles.open(test_file, 'w') as f:
            await f.write('test')
        os.remove(test_file)
        return True
    except:
        return False

async def get_save_path():
    while True:
        path = await ainput("Введите путь для сохранения изображений: ")
        path = path.strip()
        if not path:
            print("Путь не может быть пустым.")
            continue
        if not os.path.exists(path):
            print("Указанный путь не существует.")
            continue
        if not await is_path_writable(path):
            print("Нет прав на запись в эту директорию.")
            continue
        return path

def is_valid_url(url):
    return url.startswith(('http://', 'https://'))

def extract_filename(url):
    """Извлекает имя файла из URL"""
    url_without_params = url.split('?')[0]
    filename = url_without_params.split('/')[-1]
    
    if not filename or '.' not in filename:
        return None
    return filename

async def download_image(session, link, save_dir, results):
    try:
        async with session.get(link, timeout=10, allow_redirects=True) as resp:
            if resp.status == 200:
                filename = extract_filename(link)
                if not filename:
                    content_type = resp.headers.get('content-type', '')
                    if 'jpeg' in content_type or 'jpg' in content_type:
                        filename = f"image_{len(results['success'])}.jpg"
                    elif 'png' in content_type:
                        filename = f"image_{len(results['success'])}.png"
                    else:
                        filename = f"image_{len(results['success'])}.jpg"
                
                save_path = os.path.join(save_dir, filename)
                
                async with aiofiles.open(save_path, mode='wb') as f:
                    await f.write(await resp.read())
                
                results['success'].append(link)
            else:
                results['failed'].append((link, f"HTTP {resp.status}"))
    except asyncio.TimeoutError:
        results['failed'].append((link, "Таймаут"))
    except aiohttp.ClientError as e:
        results['failed'].append((link, f"Ошибка клиента: {str(e)}"))
    except Exception as e:
        results['failed'].append((link, str(e)))

async def worker(queue, save_dir, results, active_tasks, tasks_lock):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                link = await queue.get()
                async with tasks_lock:
                    active_tasks[0] += 1  
                
                await download_image(session, link, save_dir, results)
                queue.task_done()
                
                async with tasks_lock:
                    active_tasks[0] -= 1  
            except asyncio.CancelledError:
                break

async def iostream(queue: asyncio.Queue, results):
    while True:
        link = await ainput("Введите ссылку на изображение (или пустую строку для завершения): ")
        link = link.strip()
        
        if link == "":
            break
            
        if not is_valid_url(link):
            print("Ошибка ввода URL")
            results['failed'].append((link, "Некорректный URL"))
            continue
            
        await queue.put(link)

def print_results(results):
    table = PrettyTable()
    table.field_names = ["Ссылка", "Статус"]
    table.align["Ссылка"] = "l"
    table.align["Статус"] = "c"
    table.max_width["Ссылка"] = 80
    
    for url in results['success']:
        table.add_row([url, "Успех"])
    
    for url, reason in results['failed']:
        table.add_row([url, "Ошибка"])
    
    print("\n" + "="*80)
    print("Сводка об успешных и неуспешных загрузках")
    print("="*80)
    print(table)

async def manager(queue: asyncio.Queue, iostream_task: asyncio.Task, worker_task: asyncio.Task, results, active_tasks, tasks_lock):
    await iostream_task
    print('\nЗавершение работы')
    
    remaining_in_queue = queue.qsize()
    
    async with tasks_lock:
        active = active_tasks[0]
    
    total_remaining = remaining_in_queue + active
    
    if total_remaining > 0:
        print(f"Ожидание загрузки {total_remaining} изображение...")
    
    await queue.join()
    
    while True:
        async with tasks_lock:
            if active_tasks[0] == 0:
                break
        await asyncio.sleep(0.1)
    
    worker_task.cancel()
    
    with suppress(asyncio.CancelledError):
        await worker_task
    
    print_results(results)

async def main():
    save_dir = await get_save_path()
    results = {'success': [], 'failed': []}
    active_tasks = [0] 
    tasks_lock = asyncio.Lock()
    
    queue = asyncio.Queue()
    
    worker_task = asyncio.create_task(worker(queue, save_dir, results, active_tasks, tasks_lock))
    iostream_task = asyncio.create_task(iostream(queue, results))
    manager_task = asyncio.create_task(manager(queue, iostream_task, worker_task, results, active_tasks, tasks_lock))
    
    print('Начало работы')
    try:
        await asyncio.gather(iostream_task, worker_task, manager_task)
    except asyncio.CancelledError:
        pass
    print('Конец работы')

if __name__ == "__main__":
    asyncio.run(main())