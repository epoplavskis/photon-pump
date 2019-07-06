BLACK_EXCLUSION=photonpump/__init__.py|photonpump/_version.py|versioneer.py
default: fast_tests
travis: init check_lint all_tests

init:
	pip install pipenv
	pipenv install --dev

fast_tests: lint
	pipenv run pytest test/conversations/

all_tests:
	pipenv run pytest

lint:
	pipenv run black . --exclude "${BLACK_EXCLUSION}"

check_lint:
	pipenv run black --check . --exclude "${BLACK_EXCLUSION}"

continous_test:
	PYASYNCIODEBUG=1 pipenv run ptw

eventstore_docker:
	docker run -d -p 2113:2113 -p 1113:1113 eventstore/eventstore
