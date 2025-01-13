#!/bin/bash

# Set default admin credentials if not provided via environment variables
ADMIN_USERNAME=${SUPERSET_ADMIN_USERNAME:-admin}
ADMIN_EMAIL=${SUPERSET_ADMIN_EMAIL:-admin@superset.com}
ADMIN_PASSWORD=${SUPERSET_ADMIN_PASSWORD:-admin}

# Create the admin user
superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname "Superset" \
    --lastname "Admin" \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD"

# Upgrade the database schema
superset db upgrade

# Initialize Superset
superset init

# Start the Superset server
exec /usr/bin/run-server.sh
