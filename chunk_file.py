import os
import multiprocessing
from collections import namedtuple

FileIndex = namedtuple('FileIndex', ['start_index', 'end_index'])

def chunk_file(file_path, num_chunks):    
    chunk_indexes = []
    current_chunk_index = 0
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_chunks
    print(file_size)    
    print(chunk_size)
    for chunk_index in range(num_chunks) :
        with open(file_path, 'rb') as file:
            file.seek(current_chunk_index + chunk_size)
            buffer = file.read(128)
            
            first_newline_index = buffer.find(b'\n')
            #print(buffer.decode('utf8'))
            current_end_index = current_chunk_index + chunk_size + first_newline_index + 1
            idx = FileIndex(current_chunk_index, current_end_index if current_end_index < file_size else file_size)
            chunk_indexes.append(idx)
            current_chunk_index = idx.end_index
            print(idx)

    return chunk_indexes 

if __name__ == "__main__":
    file_path = 'E:\\1brc_data\\measurements_100000000.txt'    
    num_processes = multiprocessing.cpu_count()
    chunk_file(file_path, 4)
