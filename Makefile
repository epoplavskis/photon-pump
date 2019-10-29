BLACK_EXCLUSION=photonpump/__init__.py|photonpump/_version.py|versioneer.py|.tox|.venv
default: fast_tests
travis: check_lint tox

init:
	pip install -r requirements.txt
	pip install -e .

fast_tests: lint
	pytest test/conversations/

all_tests:
	pytest test/

tox:
	tox

lint:
	black . --exclude "${BLACK_EXCLUSION}"

check_lint:
	black --check . --exclude "${BLACK_EXCLUSION}"

continous_test:
	PYASYNCIODEBUG=1 ptw

eventstore_docker:
	docker run -d -p 2113:2113 -p 1113:1113 eventstore/eventstore
