fast_tests: lint
	pytest test/conversations/
all_tests:
	pytest
lint:
	flake8
