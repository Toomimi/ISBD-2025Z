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
