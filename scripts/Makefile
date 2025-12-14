APP_NAME = backend
BUILD_DIR = ./build

.PHONY: build run clean

all: build

build:
 @echo "+ $@"
 @echo "Creating folder $(BUILD_DIR)..."
 @mkdir -p $(BUILD_DIR)
 @go build -o $(BUILD_DIR)/worker-service ./cmd/main.go

run: build
 @echo "Running $(APP_NAME)..."
 @$(BUILD_DIR)/$(APP_NAME)

clean:
 @echo "Clearing..."
 @rm -rf $(BUILD_DIR)