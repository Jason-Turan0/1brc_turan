file_path = 'E:\\1brc_data\\measurements_100000.txt'


with open(file_path, 'r', encoding='utf-8') as f:
    for line in iter(f.readline):
        print (line)