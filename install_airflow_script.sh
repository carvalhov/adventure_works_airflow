#!/bin/bash

# Function to rollback and exit on error
rollback_and_exit() {
    echo "An error occurred. Rolling back..."
    deactivate
    exit 1
}

echo "Updating package lists..."
sudo apt update || rollback_and_exit

echo "Installing dependencies..."
sudo apt install -y python3 python3-pip python3-venv || rollback_and_exit

echo "Creating a Virtual Environment..."
python3 -m venv airflow-venv || rollback_and_exit
source airflow-venv/bin/activate || rollback_and_exit
echo "Virtual environment created and activated."

SOURCE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
export AIRFLOW_HOME=${SOURCE_DIR}/airflow-data

echo "Installing Apache Airflow..."
pip install apache-airflow || rollback_and_exit
echo "Apache Airflow installed."

echo "Initializing standalone..."
airflow standalone || rollback_and_exit
echo "Database initialized."

echo "Starting the Web Server and Scheduler..."
nohup airflow webserver --port 8080 > airflow_webserver.log 2>&1 &
nohup airflow scheduler > airflow_scheduler.log 2>&1 &
echo "Web server and scheduler started."

echo "Access the Airflow Web UI by opening a web browser and navigating to http://localhost:8080"

# Trap Ctrl+C to stop the services and clean up
trap "deactivate; exit 0" INT
wait

# chmod +x install_airflow_script.sh
# ./install_airflow_script.sh