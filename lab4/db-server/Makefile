.PHONY: all docker clean run build

BINARY_NAME=server
DOCKER_IMAGE=isbd-dbms

all: build

build:
	go build -o $(BINARY_NAME) .

run:
	go run main.go

docker:
	docker build -t $(DOCKER_IMAGE) .

run-docker:
	mkdir -p ./data ./.dbms_data
	docker run -p 8080:8080 \
		-v "$(CURDIR)/.dbms_data:/app/.dbms_data" \
		-v "$(CURDIR)/data:/data" \
		$(DOCKER_IMAGE)

clean:
	rm -f $(BINARY_NAME)

test:
	@rm -rf ./.dbms_data/*
	@docker run -d --name isbd-server -p 8080:8080 \
        -v "$(CURDIR)/.dbms_data:/app/.dbms_data" \
        -v "$(CURDIR)/data:/data" \
        $(DOCKER_IMAGE) > /dev/null
	@sleep 2
	@echo "Running tests..."
	@-(cd "../../../ISBD-MIMUW/pit" && \
		go test ./tests -interface-version 2  > "$(CURDIR)/results.txt" 2>&1) || true
	@echo ""
	@echo "========================================"
	@echo "           FINAL TEST RESULTS           "
	@echo "========================================"
	@grep -E "Total:|Passed:|Failed:" "$(CURDIR)/results.txt" || echo "All tests passed! But check results.txt"
	@echo "----------------------------------------"
	@echo "Full log saved to: results.txt"


stop-docker:
	@-docker stop isbd-server > /dev/null 2>&1 || true
	@-docker rm isbd-server > /dev/null 2>&1 || true

dev-test: docker stop-docker test

dev-test-quick: stop-docker test

full-test: docker test stop-docker