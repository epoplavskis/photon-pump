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
	pipenv run black .

check_lint:
	pipenv run black --check .

continous_test:
	PYASYNCIODEBUG=1 pipenv run ptw
