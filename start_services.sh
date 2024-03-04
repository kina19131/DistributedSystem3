#!/bin/bash

# Start the ECS client
echo "Starting ECS client..."
java -jar m2-ecs.jar &
echo $! > ecs.pid

sleep 1

# Number of server nodes to start
N=$1

# Check if number of nodes is provided
if [ -z "$N" ]
then
  echo "Please specify the number of server nodes to start."
  exit 1
fi

# Start N server nodes
for ((i=0; i<N; i++))
do
  NODENAME="Node_$i"
  PORT=$((50000 + i))
  echo "Starting server $NODENAME on port $PORT..."
  java -jar m2-server.jar -n $NODENAME -p $PORT &
  echo $! > "server_$i.pid"
done

echo "All services started."
