.PHONY: proto build clean test integration-test demo

PROTO_DIR  := proto
PB_DIR     := proto/kvpb
PROTO_FILE := $(PROTO_DIR)/kvstore.proto

proto:
	@mkdir -p $(PB_DIR)
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--proto_path=$(PROTO_DIR) \
		-I$(PROTO_DIR) \
		$(PROTO_FILE)
	@# protoc puts files relative to go_package; move if needed
	@if [ -d "dkv/proto/kvpb" ]; then mv dkv/proto/kvpb/*.go $(PB_DIR)/ && rm -rf dkv; fi
	@echo "Proto generated in $(PB_DIR)/"

build: proto
	go build -o bin/dkv-node   ./cmd/node
	go build -o bin/dkv-cli    ./cmd/cli
	@echo "Binaries in bin/"

build-only:
	go build -o bin/dkv-node   ./cmd/node
	go build -o bin/dkv-cli    ./cmd/cli
	@echo "Binaries in bin/"

clean:
	rm -rf bin/ data/ $(PB_DIR)/*.go

test:
	go test ./internal/... -v -count=1 -timeout 60s

integration-test:
	go test ./integration/... -v -count=1 -timeout 120s

demo: build
	@echo "Running basic demo scenario..."
	./bin/dkv-cli scenario basic

fmt:
	go fmt ./...

vet:
	go vet ./...
