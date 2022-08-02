BLACK_EXCLUSION=photonpump/__init__.py|photonpump/_version.py|versioneer.py|.tox|.venv
SHELL = /bin/bash
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

cleanup:
	- docker rm -f eventstore_local
	- docker rm -f eventstore_local_noauth

run_compose:
	docker-compose up -d


create_users:
	curl -k -f -i "https://127.0.0.1:2111/streams/%24settings" \
		--user admin:changeit \
    	-H "Content-Type: application/vnd.eventstore.events+json" \
    	-d @default-acl.json
	curl -k -f -i "https://127.0.0.1:2111/users" \
    	--user admin:changeit \
    	-H "Content-Type: application/json" \
    	-d @test-user.json

eventstore_docker: run_compose create_users
