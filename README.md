<h3>Network Topology Analysis</h3>
A network topology consists of many nodes (or hosts) and edges (connections) that link each of the nodes. In communication systems, there are typically many routes that we can take to get from Point A to Point B. 
<br>
<br>For example, if you are on your home wifi (Point A) and you request a webpage from Google.com (Point B), then your request will be relayed through many hosts along the route. Each time you make the request, a slightly different path may be used based on how the network is optimized, timeouts, failed nodes, etc. 
<br>
<br>This repo contains the code used to collect network topology data (using a traceroute script), the Apache Spark code used for the analysis, and the Zeppelin notebook used for data visualization. 
<br>
<br><b>References:</b>
<br>
