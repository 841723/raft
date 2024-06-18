#!/bin/bash

# Lista de IPs y puertos a los que quieres conectarte
IPs=("192.168.3.1" "192.168.3.2" "192.168.3.3")
Puertos=('29251,29252,29253')
pids=$(lsof -t -i :29251,29252,29253)


# Itera sobre las IPs y puertos
for ((i=0; i<${#IPs[@]}; i++)); do
    # Conectarse a la máquina remota y matar el proceso en el puerto especificado
    #ssh ${IPs[$i]} "kill -9 \$pids"
    pids=$(ssh -n ${IPs[$i]} "lsof -t -i:$Puertos")
    
    if [ -n "$pids" ]; then
        echo "killing $pids from ${IPs[$i]}"
        ssh -n ${IPs[$i]} "kill -9 $pids"

    else
        echo "NO pids from ${IPs[$i]}"
    fi
    
done