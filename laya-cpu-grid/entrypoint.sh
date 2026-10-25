#!/bin/bash
set -e

# 1. Correzione permessi: Assicuriamoci che l'utente layauser possa scrivere nella cache
if [ -d "/data/huggingface" ]; then
    echo "[INFO] [Entrypoint] Fixing permissions for /data/huggingface..."
    chown -R layauser:layauser /data/huggingface
fi

# 2. Esecuzione: Passiamo il controllo a gosu, che esegue il comando come layauser
# 'exec' assicura che il comando diventi il PID 1, gestendo correttamente i segnali (SIGTERM)
echo "[INFO] [Entrypoint] Dropping privileges and executing: $@"
exec gosu layauser "$@"
