.PHONY: all analyze generator clean

all: analyze generator

analyze:
	cd tomy_validator && go build -o ../analyzer ./cmd/analyzer

generator:
	cd tomy_validator && go build -o ../generator ./cmd/generator

clean:
	rm -f analyzer generator
