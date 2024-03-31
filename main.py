from statistics import mean
import timeit
from chunk_file import chunk_file
import multiprocessing
import os

def process_chunk(file_path, start_index, end_index, result_queue):
    city_temps = {}

    processed_count =0
    with open(file_path, 'r', encoding='utf-8') as f:
        f.seek(start_index)
        for line in iter(f.readline, ''):
            if f.tell() > end_index:
                break

            split = line.split(';')
            city = split[0]
            temperature = float(split[1])
            if not city in city_temps:
                city_temps[city] = {
                    'count': 0,
                    'sum': 0
                }
            
            city_temps[city] = {
                'count': city_temps[city]['count'] +1,
                'sum': city_temps[city]['sum'] + temperature,
            }
            processed_count +=1
    print(f'Processed {processed_count} Cities:{len(city_temps.keys())}')
    result_queue.put((city_temps, processed_count))
    

def main():    
    file_path = 'E:\\1brc_data\\measurements_100000000.txt'
    num_processes = 8
    file_indexes = chunk_file(file_path, num_processes)
    result_queue = multiprocessing.Queue()
    results = []
    processes = []    
    for idx in file_indexes:
        process = multiprocessing.Process(target=process_chunk, args=(file_path,idx.start_index, idx.end_index, result_queue))
        processes.append(process)
        process.start()
        #results.append(process_chunk(file_path, idx.start_index, idx.end_index))
    
    while(len(results)< num_processes):
        result = result_queue.get()
        if(result != None):
            results.append(result)

    for process in processes:
        process.join()

    print(sum([processed_count for (_,processed_count) in results]))

    # sorted_cities = list(city_temps.keys())
    # sorted_cities.sort()
    # print(f'{len(sorted_cities)} number of cities')
    # for city in sorted_cities:
    #     city_avg = mean(city_temps[city])
    #     print(f'{city}:{city_avg}:${len(city_temps[city])}')

if __name__ == '__main__':
    secs = timeit.timeit(main, number=1)
    print(f'total secs {secs}')