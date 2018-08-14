fast_tests: lint
	pipenv run pytest test/conversations/
all_tests:
	pipenv run pytest
lint:
	pipenv run black .
continous_test:
	pipenv run ptw
