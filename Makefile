HOMEDIR := $(shell pwd)
OUTDIR  := $(HOMEDIR)/output

GO      := go
GOMOD   := $(GO) mod
GOBUILD := $(GO) build
GOTEST  := $(GO) test -race -timeout 30s
GOPKGS  := $$($(GO) list ./...| grep -vE "vendor")

# make, make all
all: prepare compile package

#make prepare, download dependencies
prepare:
	$(GOMOD) download

#make compile
compile: build

build:
	$(GOBUILD) -o $(OUTDIR)/bin/session_watcher ./cmd/session-watcher

# make test, test your code
test:
	$(GOTEST) -v -cover $(GOPKGS)

# make package
package:
	mkdir -p $(OUTDIR)/bin

# make clean
clean:
	$(GO) clean
	rm -rf $(OUTDIR)

.PHONY: all prepare compile test package clean build
