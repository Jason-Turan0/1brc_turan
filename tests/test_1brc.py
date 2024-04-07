import os
import unittest
import sys 
import multiprocessing

import chunk_file
import main

class Test1brc(unittest.TestCase):
    file_path = './tests/data/measurements_10.txt'

    def parseTestFile(self, num_chunks):
        indexes = chunk_file.chunk_file(self.file_path, num_chunks)
        actual = []
        for index in indexes:
            [start_index, end_index] = index
            with open(self.file_path, 'rb') as file:                                            
                file.seek(start_index)
                chunk = file.read(end_index - start_index)
                actual.append(chunk)
        return actual

     
    def test_chunk_file(self):
        indexes = chunk_file.chunk_file(self.file_path, 5)
        print(indexes)  
        with open(self.file_path, 'rb') as file:            
            [start_index, end_index] = indexes[1]
            file.seek(start_index)
            first_line = file.readline()
            self.assertEqual(first_line.decode('utf-8'), 'Neuss;39.0\r\n')

    def test_chunks(self):
        actual = self.parseTestFile(3)       
        expected =[
            b'Babhnoul;27.3\r\nVigasio;14.3\r\nCalbuco;-24.5\r\nNeuss;39.0\r\n',
            b'Bolotnoye;-43.6\r\nSampu\xc3\xa9s;49.0\r\nKurumul;63.5\r\nSarasota Springs;-50.5\r\n',
            b'Bolotnoye;82.6\r\nG\xc5\xabd\xc5\xabru;89.6\r\n'
        ]      
        self.assertEqual(expected, actual)

    def test_chunks_2(self):     
        expected =[
            b'Babhnoul;27.3\r\nVigasio;14.3\r\n', 
            b'Calbuco;-24.5\r\nNeuss;39.0\r\nBolotnoye;-43.6\r\n', 
            b'Sampu\xc3\xa9s;49.0\r\nKurumul;63.5\r\n', 
            b'Sarasota Springs;-50.5\r\nBolotnoye;82.6\r\n', 
            b'G\xc5\xabd\xc5\xabru;89.6\r\n', 
            b'']
        actual = self.parseTestFile(6)
        print('testchunk2')
        print(actual)
        self.assertEqual(expected, actual)

    def test_chunks_3(self):    
        expected = [
            b'Babhnoul;27.3\r\nVigasio;14.3\r\nCalbuco;-24.5\r\nNeuss;39.0\r\nBolotnoye;-43.6\r\nSampu\xc3\xa9s;49.0\r\n', 
            b'Kurumul;63.5\r\nSarasota Springs;-50.5\r\nBolotnoye;82.6\r\nG\xc5\xabd\xc5\xabru;89.6\r\n'
        ]    
        actual = self.parseTestFile(2)        
        self.assertEqual(expected, actual)                            

    def test_process_chunk(self):       
        file_indexes = chunk_file.chunk_file(self.file_path, 6)
        result_queue = multiprocessing.Queue()
        print(file_indexes)
        results = []
        for (chunk_index, idx) in enumerate(file_indexes): 
            result = main.process_chunk(self.file_path, chunk_index, idx.start_index, idx.end_index, result_queue)
            results.append(result)
        
        [city_averages, total_processed_count] = main.finalize_results(results)
        expected_city_averages = {
            'Babhnoul': 27.3,
            'Bolotnoye':  19.499999999999996,
            'Calbuco': -24.5,
            'Kurumul': 63.5,
            'Neuss': 39.0,
            'Sampués': 49.0,
            'Sarasota Springs': -50.5,
            'Vigasio': 14.3,
            'Gūdūru': 89.6
        }
        print(city_averages)
        self.assertEqual(expected_city_averages, city_averages)
        self.assertEqual(total_processed_count, 10)


        
        
