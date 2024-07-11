## Xlwings config:
- xlwings: pip install --upgrade xlwings pywin32 --user

1. Abrir cmd
- pip install -r excel_functions\requirements.txt
- xlwings addin install
2. Abrir um excel, salvar como xslm
- Open up the Developer console (Alt-F11) Click on Tools -> References and select xlwings
- No addin do xlwings:
- Interpreter: C:\Python37\python.exe
- PYTHONPATH: excel_functions
- UDF Modules: excel_functions
- Click on "Import Functions"
3. Try: =PH_WEIGHTS("IBOV Index")

## Instructions:
start:
	docker compose up --build -d
stop:
	docker compose down

docker exec -it ID_CONTAINER /bin/bash
pg_dump -U username -d dbname -f NOME_DO_ARQUIVO_DUMP.sql
docker cp 4f6981d8adeb:backup_2024-07-09.sql.gz C:\Users\leona\Projects\backup-ph