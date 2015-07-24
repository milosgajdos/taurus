FRAMEWORK=taurus
CLIENT=tcli
BUILD=go build
CLEAN=go clean
INSTALL=go install
SRCPATH=./cmd
BUILDPATH=./build

framework: build
		$(BUILD) -v -o $(BUILDPATH)/$(FRAMEWORK) $(SRCPATH)/framework
client: build
		$(BUILD) -v -o $(BUILDPATH)/$(CLIENT) $(SRCPATH)/cli

all: build framework client

install:
		$(INSTALL) $(SRCPATH)/...
clean:
		rm -rf $(BUILDPATH)
build:
		mkdir -p $(BUILDPATH)

.PHONY: clean build
