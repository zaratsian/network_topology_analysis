# Build:
docker build -t phoenix .

# Run with only Query Server port exposed:
docker run -d -p 8765:8765 phoenix

# Run exposing HBase ports for thick JDBC client
docker run -d -p 8765:8765 -p 2181:2181 -p 16010:16010 -p 16020:16020 -p 16030:16030 --hostname phoenix phoenix

# Add container hostname to /etc/hosts
sudo echo "127.0.0.1  phoenix" >> /etc/hosts

