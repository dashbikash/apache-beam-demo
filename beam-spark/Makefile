.PHONY: build

build:
	./gradlew --no-configuration-cache clean build
	docker build -t beam-spark -f Dockerfile .