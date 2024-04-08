import timeit
from chunk_file import chunk_file
import multiprocessing
import json 
import os  
import atexit
import line_profiler
import ctypes
from fastnumbers import float as fast_float
c_lib = ctypes.cdll.msvcrt
strtok = c_lib.strtok
strtok.restype = ctypes.c_char_p
strtok.argtypes = [ctypes.c_char_p, ctypes.c_char_p]

# profile = line_profiler.LineProfiler()
# atexit.register(profile.print_stats)

newline_1 = '\r'
newline_2 = '\n'
seperator = ';'
delimiter = ';'.encode('utf-8')
newline_char = os.linesep
newline_char_encode = os.linesep.encode('utf-8')
newline_char_len =len(newline_char)
buffer_size = 1024

# def get_lines(buffer, unprocessed_text, is_last_buffer):
#     text_data = unprocessed_text + buffer.decode('utf-8')
#     if(is_last_buffer == True):
#         return (text_data, '')
    
#     last_newline_index = text_data.rfind(newline_char)    
#     indexToProcess = last_newline_index + newline_char_len
#     text_data_to_process = text_data[0:indexToProcess]
#     unprocessed_text = text_data[indexToProcess: len(text_data)]    
#     return (text_data_to_process, unprocessed_text)

#@profile
def process_line(line, city_temps):
    split = line.split(';')    
    if not split[0] in city_temps:
        city_temps[split[0]] = {
            'count': 0,
            'sum': 0
        }
    city_temps[split[0]]['count'] += 1
    city_temps[split[0]]['sum'] += float(split[1])    

#@profile
# def process_line(line, city_temps):
#     split = line.split(';')    
#     city_temps[f'{split[0]}_count'] = city_temps.get(f'{split[0]}_count',0) + 1
#     city_temps[f'{split[0]}_sum'] = city_temps.get(f'{split[0]}_sum', 0) + float(split[1])        


#@profile
def parse_buffer(buffer, city_temps):
    text_data = buffer.decode('utf-8')
    lines = text_data.splitlines()
    processed_count =0
    for line_index in range(len(lines) -1): 
        line = lines[line_index]        
        process_line(line, city_temps)       
        processed_count += 1

    last_line = lines[len(lines) -1]
    if(buffer.endswith('\n'.encode('utf-8'))):
        process_line(last_line, city_temps)       
        processed_count += 1
        return (None, processed_count)

    return (last_line.encode('utf-8'), processed_count)


def parse_buffer_bad(buffer, unprocessed_text, is_last_buffer, city_temps):
    text_data = unprocessed_text + buffer.decode('utf-8')
    #data_len = len(text_data)
    current_city = ''
    current_temperature = ''
    parsing_temperature = False
    parsed_index = 0
    parse_count = 0
    for (char_index, char) in enumerate(text_data):        
        # is_last_char = char_index == data_len - 1
        # should_process_line = char == newline_2 or (is_last_char and is_last_buffer)
        # if is_last_char and is_last_buffer:
        #     current_temperature += char
        
        if char == newline_2:
            temperature = float(current_temperature)
            if not current_city in city_temps:
                city_temps[current_city] = {
                    'count': 0,
                    'sum': 0
                }            
            city_temps[current_city] = {
                'count': city_temps[current_city]['count'] +1,
                'sum': city_temps[current_city]['sum'] + temperature,
            }            
            current_city = ''
            current_temperature =''
            parsing_temperature = False
            parsed_index = char_index
            parse_count +=1 
        elif char == seperator:
            parsing_temperature = True
        elif not parsing_temperature:
            current_city += char
        elif parsing_temperature:
            current_temperature += char

    unprocessed_text = text_data[parsed_index +1 : len(text_data)]
    return (unprocessed_text, parse_count)


#@profile
def process_line_new(line, city_temps):
    city = strtok(line, delimiter).decode('utf-8')
    temp = strtok(None, delimiter)
    #cities.append({'city': city, 'temp': float(temp)})
    if not city in city_temps:
        city_temps[city] = {
            'count': 0,
            'sum': 0
        }
    city_temps[city]['count'] += 1
    city_temps[city]['sum'] += fast_float(temp)

#@profile
def parse_buffer_new(buffer, city_temps):
    c_csv_string = ctypes.c_char_p(buffer)
    line = strtok(c_csv_string, newline_char_encode)
    lines = []
    while line:        
        lines.append(line)        
        line = strtok(None, newline_char_encode)
    processed_count =0
    for line in lines[:-1]:         
        process_line_new(line, city_temps)
        processed_count += 1
        
    last_line = lines[-1]
    if(buffer.endswith('\n'.encode('utf-8'))):
        process_line_new(last_line, city_temps)       
        processed_count += 1
        return (None, processed_count)
    
    return (last_line, processed_count)


#128 10000000 in binary
highest_bit = 0x80

#Do bitwise mask to see if byte has a continuation flag on it
def is_continuation_byte(byte): 
   return (byte & highest_bit) == highest_bit
   #x = byte & 0xC0
   #are_equal = 
   #print(f'{"{:03}".format(byte)} {"{:08b}".format(byte)}')
   #print(f'{"{:03}".format(x)} {"{:08b}".format(x)}')
   #print(f'{"{:03}".format(highest_bit)} {"{:08b}".format(highest_bit)}')    
   #print(are_equal)
   #return are_equal

#@profile
def process_chunk(file_path, chunk_index, start_index, end_index, result_queue):
    city_temps = {}
    current_chunk_index = start_index
    processed_count = 0
    unprocessed_buffer = None
    with open(file_path, 'rb') as file:
        file.seek(start_index)
        while current_chunk_index < end_index:
            #print(f'current_chunk_index {current_chunk_index}')
            buffer_read_size = buffer_size if current_chunk_index + buffer_size <= end_index else end_index - current_chunk_index            
            buffer = file.read(buffer_read_size)
            current_chunk_index += buffer_read_size
            while is_continuation_byte(buffer[-1]):                              
               buffer = buffer + file.read(1)
               current_chunk_index += 1            
            parse_result = parse_buffer_new(
                buffer if unprocessed_buffer is None else unprocessed_buffer + buffer,                 
                city_temps) 
            unprocessed_buffer = parse_result[0]
            processed_count += parse_result[1]
    print(f'Processed {processed_count} Cities:{len(city_temps.keys())}')
    result = (city_temps, processed_count)
    result_queue.put(result)    
    return result

def create_average(city, results):
    total_count = sum([city_temps[city]['count'] if city in city_temps else 0 for (city_temps,_) in results])
    total_sum = sum([city_temps[city]['sum'] if city in city_temps else 0 for (city_temps,_) in results])
    return total_sum/total_count if total_count != 0 else None

def finalize_results(results):    
    total_processed_count = sum([processed_count for (_,processed_count) in results])
    print(f'total_processed_count {total_processed_count}')
    all_cities = {city for (city_temps,_) in results for city in city_temps}
    city_averages = {
        city:create_average(city, results) for city in all_cities
    }
    return [city_averages, total_processed_count]


def process_file(file_path, num_processes):
    file_indexes = chunk_file(file_path, num_processes)
    result_queue = multiprocessing.Queue()
    results = []

    if(num_processes == 1):
        file_index = file_indexes[0]
        process_chunk(file_path, 0, file_index.start_index, file_index.end_index, result_queue)
        result = result_queue.get()
        results.append(result)
    else:
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
    final_results = finalize_results(results)
    return final_results
   

def main():    
    file_size = '1000000'
    file_path = f'E:\\1brc_data\\measurements_{file_size}.txt'
    num_processes = 1
    city_averages = process_file(file_path, num_processes)
    
    out_file_path =f"results/city_averages_{file_size}.json"
    with open(out_file_path, "w", encoding="utf-8") as file:       
        json.dump(city_averages, file, indent=4)
    print(f"Saved results to {out_file_path}")
    

if __name__ == '__main__':    
    secs = timeit.timeit(main, number=1)
    print(f'total secs {secs}')