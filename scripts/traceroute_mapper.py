
import os,sys,re

infile  = '/tmp/traceroute_google.txt'
outfile = '/tmp/traceroute_google_mapped.txt'

data = open(infile,'rb').read()

events = data.split('\n')

record = '' 

for i, event in enumerate(events):
    try:
        level   = event.split('|')[1].strip()
        ip      = event.split('|')[2].strip()
        if str(level) == '1':
            record = record + '\n' + ip
        else:
            record = record + '|' + ip
    except:
        pass

output = open(outfile,'wb')
output.write(record)
output.close()

#ZEND
