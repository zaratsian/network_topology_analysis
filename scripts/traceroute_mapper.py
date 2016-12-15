
import os,sys,re

infile  = 'traceroute_google.txt'

outfile_topology_map = 'traceroute_google_mapped.txt'
output_topology_map  = open(outfile_topology_map,'wb')

outfile_node_info    = 'traceroute_google_node_detail.txt'
output_node_info     = open(outfile_node_info,'wb')

data = open(infile,'rb').read()

events = data.split('\n')

topology_map = '' 
node_details = []

for i, event in enumerate(events):
    try:
        level     = str(event.split('|')[1].strip())
        ip        = event.split('|')[2].strip()
        country   = event.split('|')[4].strip()
        region    = event.split('|')[5].strip()
        city      = event.split('|')[6].strip()
        longitude = event.split('|')[7].strip()
        latitude  = event.split('|')[8].strip()
        isp       = event.split('|')[9].strip()
        org       = event.split('|')[10].strip()
        
        if str(level) == '1':
            topology_map = topology_map + '\n' + ip
        else:
            topology_map = topology_map + '|' + ip
        
        node_detail = ip + '|' + country + '|' + region + '|' + city + '|' + longitude + '|' + latitude + '|' + isp + '|' + org + '|' + level + '\n'
        
        # If localhost, then manually set my "home" ip information
        if re.search('192.168.0.1',node_detail):
            node_detail = 'localhost|United States|NC|Raleigh|-78.6382|35.7796|Time Warner Cable|Time Warner Cable'
        
        if node_detail not in node_details:
            node_details.append(node_detail)
            output_node_info.write(node_detail)
    except:
        pass

topology_map = topology_map.replace('|ip_address\n','').replace('192.168.0.1','localhost')

output_topology_map.write(topology_map)
output_topology_map.close()

#ZEND
