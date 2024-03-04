#!/bin/bash

# Shut down all server nodes
for pid_file in server_*.pid
do
  if [ -f "$pid_file" ]; then
    PID=$(cat "$pid_file")
    echo "Shutting down server with PID $PID..."
    kill $PID
    rm "$pid_file"
  fi
done

# Shut down the ECS client
if [ -f ecs.pid ]; then
  PID=$(cat ecs.pid)
  echo "Shutting down ECS client with PID $PID..."
  kill $PID
  rm ecs.pid
fi

rm /homes/c/chenam14/ece419/m2_new/ms2-group-35/src/logger/kv*

echo "All services shut down."
