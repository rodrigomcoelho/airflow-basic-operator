BUCKET=

lint:
	@autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive dags
	@autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive plugins
	@isort dags --profile black
	@isort plugins --profile black
	@black dags plugins


upload:
	@gsutil cp plugins/fisia/financeiro/operators/pandas_to_bigquery.py ${BUCKET}/plugins/fisia/financeiro/operators/pandas_to_bigquery.py
	@gsutil cp plugins/fisia/financeiro/operators/templates/pandas_to_bigquery.j2 ${BUCKET}/plugins/fisia/financeiro/operators/templates/pandas_to_bigquery.j2
	@gsutil cp dags/sample_001.py ${BUCKET}/dags/fisia/financeiro/sample_001.py
	@gsutil cp dags/dataframe/mycode.py ${BUCKET}/dags/fisia/financeiro/dataframe/mycode.py
	@echo "Uploaded."
