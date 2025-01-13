FROM apache/superset:latest

# Switch to root user to install dependencies
USER root

# Install PostgreSQL driver
RUN pip install psycopg2-binary

# Switch back to the default Superset user
USER superset


COPY ./superset-init.sh /superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT [ "/superset-init.sh" ]