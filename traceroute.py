
import re
import subprocess
import requests
import json
import time

hostname   = 'google.com'

file       = '/Users/dzaratsian/Dropbox/data/traceroute_google.txt'
outputfile = open(file,'wb')
outputfile.write('iteration|level|ip_address|response_time|country|region|city|longitude|latitude|isp|org\n')
outputfile.close()

for iteration in range(1,100):
    traceroute = subprocess.Popen(["traceroute",hostname],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    outputfile = open(file,'a')
    level = 0
    for line in iter(traceroute.stdout.readline,""):
        time.sleep(1)
        try:
            ip_address    = re.findall('\(.*?\)',line)[0].replace('(','').replace(')','')
            response_time = re.findall('[0-9\.]+ ms',line)[0] 
            try:
                ipdata      = json.loads(requests.get('http://ip-api.com/json/' + ip_address).content)
                country     = str(ipdata['country']).strip()
                region      = str(ipdata['region']).strip()
                zipcode     = str(ipdata['zip']).strip()
                longitude   = str(ipdata['lon']).strip()
                latitude    = str(ipdata['lat']).strip()
                isp         = str(ipdata['isp']).strip()
                org         = str(ipdata['org']).strip()
                city        = str(ipdata['city']).strip()
            except:
                country     = ''
                region      = ''
                zipcode     = ''
                longitude   = ''
                latitude    = ''
                isp         = ''
                org         = ''
                city        = ''
            
            level += 1
            
            print '[ ROUTE ] ' + str(iteration) + '|' + str(level) + '|' + ip_address + '|' + response_time + '|' + country + '|' + region + '|' + city + '|' + longitude + '|' + latitude + '|' + isp + '|' + org + '\n'
            outputfile.write(str(iteration) + '|' + str(level) + '|' + ip_address + '|' + response_time + '|' + country + '|' + region + '|' + city + '|' + longitude + '|' + latitude + '|' + isp + '|' + org + '\n')
        except:
            print '[ PASSED ]'
    
    outputfile.close()
    traceroute.kill()

print '[ INFO ] Completed - File create at ' + str(file)

#ZEND