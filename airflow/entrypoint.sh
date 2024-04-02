#!/bin/bash
set -e

# Attendez que Postgres soit prêt
while ! nc -z postgres 5432; do
  sleep 0.1
done

# Initialisation de la base de données Redash
if [ "$1" = 'server' ]; then
  echo "Initialisation de la base de données Redash..."
  flask db upgrade
fi

# Exécutez la commande par défaut (passée par CMD dans Dockerfile ou docker-compose.yml)
exec "$@"
