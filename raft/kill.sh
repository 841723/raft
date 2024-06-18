#!/bin/bash

# Buscar procesos que utilizan los puertos 29001, 29002 y 29003
pids=$(lsof -t -i :29001,29002,29003)

# Verificar si se encontraron PIDs
if [ -n "$pids" ]; then
  echo "Procesos encontrados en los puertos 29001, 29002, 29003. Matando procesos..."
  # Matar los procesos con kill -9
  kill -9 $pids
  echo "Procesos terminados."
else
  echo "No se encontraron procesos en los puertos 29001, 29002, 29003."
fi