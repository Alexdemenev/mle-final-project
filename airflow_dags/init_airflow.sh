mkdir -p ./dags ./logs ./plugins ./config
echo -e "\nAIRFLOW_UID=$(id -u)" >> .env
docker compose up airflow-init 
docker compose down --volumes --remove-orphans 