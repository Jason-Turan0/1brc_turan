import os
import ctypes
import timeit
from fastnumbers import float as fast_float
c_lib = ctypes.cdll.msvcrt

# Define the strtok function prototype
strtok = c_lib.strtok
strtok.restype = ctypes.c_char_p
strtok.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
#print(c_lib.strtod)
delimiter = ';'.encode('utf-8')
newline_char = os.linesep.encode('utf-8')
file_path = './tests/data/measurements_10.txt'
def main():
    with open(file_path, 'rb') as file:                                                
        chunk = file.read(1024)
        c_csv_string = ctypes.c_char_p(chunk)
        line = strtok(c_csv_string, newline_char)
        
        # List to store extracted values
        lines = []
        cities = []        
        # Iterate through the tokens
        while line:        
            lines.append(line)        
            line = strtok(None, newline_char)

        for line in lines:
            city = strtok(line, delimiter)
            temp = strtok(None, delimiter)
            cities.append({'city': city, 'temp': float(temp)})
        print(cities)
    



if __name__ == '__main__':    
    secs = timeit.timeit(main, number=1)
    print(f'total secs {secs}')