include Makefile.variables
include Makefile.precommit
include Makefile.docker

DOCKER_REGISTRY ?= docker.io
IMAGE ?= bborbe/agent-task-executor
ifeq ($(VERSION),)
	VERSION := $(shell git describe --tags `git rev-list --tags --max-count=1`)
endif

run:
	@go run -mod=mod main.go -v=2

deps:
	go install github.com/onsi/ginkgo/v2/ginkgo@v2.25.3
	sudo port install trivy

.PHONY: fix
fix:
	@for dir in $$(find `pwd` -type d -name vendor -prune -o -name go.mod -exec dirname "{}" \; | grep -v '^$$'); do \
		cd $${dir}; \
		echo "fix $${dir}"; \
		go get \
		github.com/bborbe/kv@latest \
		github.com/bborbe/memorykv@latest \
		github.com/bborbe/badgerkv@latest \
		github.com/bborbe/boltkv@latest \
		github.com/go-git/go-git/v5@latest \
		github.com/containerd/containerd@latest \
		golang.org/x/crypto@latest \
		golang.org/x/net@latest; \
	done
