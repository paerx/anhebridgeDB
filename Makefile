BIN_DIR := bin

.PHONY: build build-server build-client build-all test clean docker-build dashboard-build dashboard-install dashboard-dev

build: build-server

build-server:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/anhe-server ./cmd/server

build-client:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/anhe-client ./cmd/client

build-all: build-server build-client
	go build ./cmd/stress ./cmd/sdkdemo
	$(MAKE) dashboard-build

dashboard-install:
	cd dashboard && npm ci

dashboard-build:
	cd dashboard && npm run build

dashboard-dev:
	cd dashboard && npm run dev

test:
	go test ./...

docker-build:
	docker build -t anhebridgedb:latest .

clean:
	rm -rf $(BIN_DIR)
	rm -rf dashboard/dist
