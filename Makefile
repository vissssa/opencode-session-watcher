# 百度 Go 编译环境使用指南 https://ku.baidu-int.com/d/SzGt0sD37hWmmp

HOMEDIR := $(shell pwd)
OUTDIR  := $(HOMEDIR)/output

# 设置编译时所需要的 go 环境
export GOENV = $(HOMEDIR)/go.env

GOPKGS  := $$(go list ./...| grep -vE "vendor")

# make, make all
all: prepare compile package

prepare:
	git version     # 低于 2.17.1 可能不能正常工作
	go env          # 打印出 go 环境信息，可用于排查问题
	go mod download || go mod download -x  # 下载 依赖

#make compile
compile: build
build:
	go build -o $(HOMEDIR)/session_watcher ./cmd/session-watcher

# make test, test your code
test: prepare
	go test -race -timeout=120s -v -cover $(GOPKGS) -coverprofile=coverage.out | tee unittest.txt

# make package
package:
	rm -rf $(OUTDIR)
	mkdir -p $(OUTDIR)
	mv session_watcher  $(OUTDIR)/

# make clean
clean:
	go clean
	rm -rf $(OUTDIR)

# avoid filename conflict and speed up build
.PHONY: all prepare compile test package clean build