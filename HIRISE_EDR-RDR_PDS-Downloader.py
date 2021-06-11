#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT for download multiple HiRISE images from using filtered shapefile from PDS
@author: Giacomo Nodjoumi - g.nodjoumi@jacobs-university.de



If NO argument is passed, defaults are used and interactively requested the others.


@author: Giacomo Nodjoumi - g.nodjoumi@jacobs-university.de
"""

import os
import pandas as pd
from tqdm import tqdm
from utils.GenUtils import chunk_creator, parallel_funcs, readGPKG
from utils.FileUtils import getFileUrl, getFile
import psutil
from argparse import ArgumentParser
from tkinter import Tk,filedialog
global dst_folder
global gpkgDF


def main(gpkgDF):
    JOBS=psutil.cpu_count(logical=False)
    
    try:
        file_urls = pd.read_csv(dst_folder+'/file_urls.txt', header=None)[0].tolist()
    except Exception as e:
        print(e)
        print('Download list not found, creating')
        
        download_urls = [gpkgDF[gpkgDF['ProductId']== product]['FilesURL'].values[0] for product in gpkgDF['ProductId'] if 'RED' in product] 

        chunks = []
        for c in chunk_creator(download_urls, JOBS):
            chunks.append(c)
        
        # file_urls = [getFileUrl(url) ]
        file_urls = []
        with tqdm(total=len(download_urls),
                 desc = 'Generating Images',
                 unit='File') as pbar:
            
           
            for i in range(len(chunks)):
                files = chunks[i]
                results = parallel_funcs(files, 2, getFileUrl, ext)
                pbar.update(JOBS)
                [file_urls.append(url) for url in results]
        
            df = pd.DataFrame(file_urls)
            savename = dst_folder+'/file_urls.txt'
            df.to_csv(savename, index=False, header=False)
            pass
        
        
    proc_csv = dst_folder+'/Processed.csv'
    try:
        proc_df = pd.read_csv(proc_csv)
    except Exception as e:
        print(e)
        print('Processed csv created')
        proc_df = pd.DataFrame(columns=['Name'])
    pass
        
    with tqdm(total=len(file_urls),
             desc = 'Downloading Images',
             unit='File') as pbar:
        
        filerange = len(file_urls)
        chunksize = round(filerange/JOBS)
        if chunksize <1:
            chunksize=1
            JOBS = filerange
        chunks = []
        for c in chunk_creator(file_urls, JOBS):
            chunks.append(c)
            
        for i in range(len(chunks)):
            files = chunks[i]
            lambda_f = lambda element:(os.path.basename(element).split('.')[0]) not in proc_df['Name'].to_list()
            filtered = filter(lambda_f, files)
            chunk = list(filtered)
            if len(chunk)>0:
                tmp_df = parallel_funcs(files, JOBS, getFile, dst_folder)
                for df in tmp_df:
                    proc_df = proc_df.append(df,ignore_index=True)
                proc_df.to_csv(proc_csv, index=False)
                pbar.update(JOBS)
            else:
                pbar.update(len(files))
                continue
    
    print('\nAll operations completed')


if __name__ == "__main__":
    global dst_folder
    
    parser = ArgumentParser()
    parser.add_argument('--path', help='download folder path')
    parser.add_argument('--shp', help='shapefile or geopackage with orbits')
    parser.add_argument('--ext', help='extension of desired files')
    
    args = parser.parse_args()
    path = args.path
    gpkg_file = args.shp
    ext = args.ext
    
    if  path is None:
        root = Tk()
        root.withdraw()
        dst_folder = filedialog.askdirectory(parent=root,initialdir=os.getcwd(),title="Please select the folder Download folder")
        print('Working folder:', dst_folder)
    
    if  gpkg_file is None:
        root = Tk()
        root.withdraw()
        gpkg_file = filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes = (("Esri Shapefile","*.shp"),("all files","*.*")))
        print('Working folder:', gpkg_file)
    
    if ext is None:
            print('Please enter TIFF or tiff, PNG or png or JPG or jpg, JP2. Leave empty for JP2')    
            ixt = input('Enter input image format: ')
            if ixt == '':
                ixt = '.JP2'
                 
    gpkgDF = readGPKG(gpkg_file)
    
    
    main(gpkgDF)
   



