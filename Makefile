default: fast_tests
travis: init all_tests

init:
	pip install pipenv
	pipenv install --dev

fast_tests: lint
	pipenv run pytest test/conversations/

all_tests:
	pipenv run pytest

lint:
	pipenv run black .

continous_test:
	PYASYNCIODEBUG=1 pipenv run ptw
