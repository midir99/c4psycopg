PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))


clean:
	find . -name '__pycache__' | xargs rm -rf
	rm -rf htmlcov .coverage .pytest_cache


coverage:
	$(PYTHON) -m pytest --cov="." --cov-report html


format:
	$(PYTHON) -m black e4psycopg tests
	$(PYTHON) -m isort .


lint:
	$(PYTHON) -m pylint e4psycopg


test:
	$(PYTHON) -m pytest
