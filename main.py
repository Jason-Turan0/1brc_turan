import timeit
from chunk_file import chunk_file
import multiprocessing
import json
import cProfile

buffer_size = 1024

def process_chunk(file_path, chunk_index, start_index, end_index, result_queue):
    pr = cProfile.Profile()
    pr.enable()    
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
    pr.disable()
    pr.dump_stats(f'results/1brc_turan_{chunk_index}.profile')

def create_average(city, results):
    total_count = sum([city_temps[city]['count'] if city in city_temps else 0 for (city_temps,_) in results])
    total_sum = sum([city_temps[city]['sum'] if city in city_temps else 0 for (city_temps,_) in results])
    return total_sum/total_count if total_count != 0 else None
        

def main():    
    file_size = '1000000'
    file_path = f'E:\\1brc_data\\measurements_{file_size}.txt'
    num_processes = 8
    file_indexes = chunk_file(file_path, num_processes)
    result_queue = multiprocessing.Queue()
    results = []
    processes = []    
    for (chunk_index, idx) in enumerate(file_indexes):
        process = multiprocessing.Process(target=process_chunk, args=(file_path, chunk_index, idx.start_index, idx.end_index, result_queue))
        processes.append(process)
        process.start()        
    
    while(len(results)< num_processes):
        result = result_queue.get()
        if(result != None):
            results.append(result)

    for process in processes:
        process.join()
    
    total_processed_count = sum([processed_count for (_,processed_count) in results])
    print(f'total_processed_count {total_processed_count}')
    all_cities = {city for (city_temps,_) in results for city in city_temps}
    city_averages = {
        city:create_average(city, results) for city in all_cities
    }
    out_file_path =f"results/city_averages_{file_size}.json"
    with open(out_file_path, "w", encoding="utf-8") as file:       
        json.dump(city_averages, file, indent=4)
    print(f"Saved results to {out_file_path}")
    

if __name__ == '__main__':    
    secs = timeit.timeit(main, number=1)
    print(f'total secs {secs}')