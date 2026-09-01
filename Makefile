.PHONY: help install init validate load-all load-lake load-bronze load-silver clean

help:
	@echo "EDS Pipeline - Commandes disponibles"
	@echo ""
	@echo "Setup initial :"
	@echo "  make install      - Installe les dépendances"
	@echo "  make init         - Initialise ClickHouse (bases + tables)"
	@echo ""
	@echo "Pipeline de données :"
	@echo "  make load-lake    - Copie filestorage → lake (anonymisation)"
	@echo "  make load-bronze  - Charge lake → ClickHouse bronze"
	@echo "  make load-silver  - Transforme bronze → silver (star schema)"
	@echo "  make load-all     - Exécute le pipeline complet (lake→bronze→silver)"
	@echo ""
	@echo "Validation :"
	@echo "  make validate     - Teste la connexion et la structure"
	@echo ""
	@echo "Nettoyage :"
	@echo "  make clean        - Supprime les fichiers temporaires"

install:
	python3 -m pip install -r requirements.txt

docker-up:
	docker-compose up -d
	@echo "Attendez 10 secondes..."
	sleep 10

docker-down:
	docker-compose down

init: docker-up
	@echo "\n→ Initialisation des bases et tables..."
	python3 scripts/init_clickhouse.py

validate:
	python3 scripts/validate_pipeline.py

load-lake:
	@echo "\n→ Copie filestorage → lake..."
	python3 -m src.pipeline --step lake

load-bronze:
	@echo "\n→ Chargement lake → bronze..."
	python3 -m src.pipeline --step bronze

load-silver:
	@echo "\n→ Transformation bronze → silver..."
	python3 -m src.pipeline --step silver

load-all:
	@echo "\n→ Pipeline complet..."
	python3 -m src.pipeline --step all

load-date:
	@echo "\n→ Pipeline complet (date spécifique)..."
	python3 -m src.pipeline --date $(DATE) --step all

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.tmp" -delete

test-query:
	@echo "Contage des lignes chargées..."
	python3 -c "from src.db import get_client; from datetime import date; c = get_client(); print('Patients :', c.execute('SELECT count() FROM eds_bronze.patients')[0][0]); print('Séjours :', c.execute('SELECT count() FROM eds_bronze.sejours')[0][0])"
