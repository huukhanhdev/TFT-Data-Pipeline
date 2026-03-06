#!/bin/bash
echo "Adding Superset to the project network..."

# Run DB upgrade
docker compose exec superset superset db upgrade

# Create an admin user
docker compose exec superset superset fab create-admin \
              --username admin \
              --firstname Admin \
              --lastname User \
              --email admin@superset.com \
              --password admin

# Setup default roles and permissions
docker compose exec superset superset init

echo "Superset setup is complete. You can access it at http://localhost:8088 (admin/admin)"
