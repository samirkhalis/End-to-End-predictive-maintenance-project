#!/bin/bash
set -e   # exit immediately if any command fails


#check if the requirements.txt file exists at the specified path.
#If the file exists, it upgrades pip using the system's Python installation.
#It then installs the Python packages listed in requirements.txt using pip. 
if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

# This block checks if the airflow.db file does not exist at the specified path.
# If the file does not exist, it initializes the Airflow database using airflow db init.
# After initializing the database, it creates a default admin user with the specified credentials using airflow users create.
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email elhaddi.zahra@gmail.com \
    --password admin
fi

# This command ensures that the Airflow database schema is up-to-date
# It uses the command "-v airflow " 
# to get the full path of the airflow executable and runs the db upgrade command.
$(command -v airflow) db upgrade


# This command replaces the current shell with the Airflow webserver process. 
# The exec command is used to ensure that the Airflow webserver becomes the main process of the container,
# which is important for proper signal handling and process management in Docker.
exec airflow webserver
