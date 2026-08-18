.PHONY: run test coverage demo benchmark docker

run:
	node src/server.js

test:
	node --test

coverage:
	node --test --experimental-test-coverage

demo:
	node bin/demo.js

benchmark:
	node bin/benchmark.js

docker:
	docker compose up --build
