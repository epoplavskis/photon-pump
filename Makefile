BLACK_EXCLUSION=photonpump/__init__.py|photonpump/_version.py|versioneer.py
default: fast_tests
travis: init check_lint all_tests

init:
	pip install -r requirements.txt
	pip install -e .

fast_tests: lint
	pytest test/conversations/

all_tests:
	pytest

lint:
	black . --exclude "${BLACK_EXCLUSION}"

check_lint:
	black --check . --exclude "${BLACK_EXCLUSION}"

continous_test:
	PYASYNCIODEBUG=1 ptw

eventstore_docker:
	docker run -d -p 2113:2113 -p 1113:1113 eventstore/eventstore
