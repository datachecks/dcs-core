## Run pytest for the module tests
pytest:
	set -e
	set -x
	pytest -p no:warnings ./tests/*

pytest_unit:
	set -e
	set -x
	pytest -p no:warnings --ignore=tests/integration/

pytest_integration:
	set -e
	set -x
	pytest -p no:warnings ./tests/integration/*


pytest_with_coverage:
	set -e
	set -x
	pytest -p no:warnings --cov ./tests/*
