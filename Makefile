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

eventstore_docker:
	docker run -d --name eventstore_local -p 2113:2113 -p 1113:1113 eventstore/eventstore:release-5.0.8
	docker run -d --name eventstore_local_noauth -p 22113:2113 -p 11113:1113 eventstore/eventstore:release-5.0.8
	for i in {1..10}; do curl -f -i "http://127.0.0.1:2113/users" --user admin:changeit && break || sleep 1; done
	for i in {1..10}; do curl -f -i "http://127.0.0.1:22113/users" --user admin:changeit && break || sleep 1; done
	curl -f -i "http://127.0.0.1:2113/streams/%24settings" \
		--user admin:changeit \
		-H "Content-Type: application/vnd.eventstore.events+json" \
		-d @default-acl.json
	curl -f -i "http://127.0.0.1:2113/users" \
		--user admin:changeit \
		-H "Content-Type: application/json" \
		-d @test-user.json
