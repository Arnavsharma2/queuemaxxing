.PHONY: run test coverage demo docker

run:
	node src/server.js

test:
	node --test

coverage:
	node --test --experimental-test-coverage

demo:
	node bin/demo.js

docker:
	docker compose up --build
