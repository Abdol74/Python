import argparse
from pathlib import Path
from subprocess import Popen , PIPE
import os
#import pandas as pd
import json 
import fnmatch
import pandas as pd
from datetime import datetime
import re

flag = True
while flag:
    time = datetime.now()
    #create parser instance 
    parser = argparse.ArgumentParser()
    #add arguments

    parser.add_argument("dir_path",help = "enter the directory_path")
    parser.add_argument("-u",action="store_true",default = False,dest="timeFormat")
    #parse the arguments

    args = parser.parse_args()

   #create an empty dict checksum contain filename and checksum hash value 
    checksums = {}

   #create an empty list for duplicates files
    duplicates = [] 

  #retireve list of files in directory path 


    basePath = Path(args.dir_path)
    files = [file for file in basePath.iterdir() if(file.is_file() & fnmatch.fnmatch(file,"*.json"))]
    #check the duplicates based on checksums has values 

    for filename in files:
        with Popen(['md5sum',filename],stdout = PIPE) as proc:
            checksum = proc.stdout.read().split()[0]
            if(checksum in checksums):
                duplicates.append(filename)
                print(" THERE IS AN DUPLICATE OF IT   {}".format(filename))
            else:
                checksums[checksum] = filename
    print(f"found duplicates : {duplicates}")

    for filename in duplicates:
        os.remove(filename)
    
    for file in files:
        if(file not in duplicates):
            data = [json.loads(line) for line in open(file,'r') if('_heartbeat_' not in json.loads(line))]
            df = pd.json_normalize(data)
            df = df.dropna()
            #The web browser that has requested the service
            web_browser = df['a'].str.split(' ',expand = True,n = 1)[0]
            df['web_browser'] = web_browser
            #operating system that intiated this request
            os = df['a'].str.split("(" , expand = True , n = 1)
            os = os[1].str.split(" ", expand = True , n = 1)
            os = os[0].str.split(";" , expand = True , n = 1)
            df['operating_sys'] = os[0]
            #The main URL the user came from
            from_url = df['r'].str.split("//",expand = True , n = 1)[1]
            from_url = from_url.str.split("/" , expand = True , n = 1)[0]
            df['from_url'] = from_url
            #The same applied like `to_url`
            to_url = df['u'].str.split("//" , expand = True , n = 1)[1]
            to_url = to_url.str.split("/" , expand = True , n = 1)[0]
            df['to_url'] = to_url
            #The city from which the the request was sent
            df['city'] = df['cy']
            #The longitude where the request was sent
            df['longitude'] = df['ll'].str[0]
            #The latitude where the request was sent
            df['latitude'] = df['ll'].str[1]
            #The time zone that the city follow
            df['time_zone'] = df['tz']
            # Time when the request started
            if(args.timeFormat):
                df['time_in'] = df['t']
                df['time_out'] = df['hc']
            else:
                df['time_in'] = pd.to_datetime(df['t'])
                df['time_out'] =pd.to_datetime(df['hc'])
            df =df[
            ['web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude', 'latitude', 'time_zone',
            'time_in', 'time_out', ]]

            print('there are {}  rows transformed from file {} '.format(df.shape[0], args.dir_path ))
            new_file = str(file).replace('.json',' ')
            df.to_csv('.//target/'+new_file+'.csv')

                
    total_excutation_time = (datetime.now() - time)
                
    print('Total Execuation Time once run a script   {}'.format(total_excutation_time))

    flag  = False

