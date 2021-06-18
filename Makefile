.PHONY: fmt lint golint test test-with-coverage ci

PACKAGES=`go list ./...`

fmt:
	for pkg in ${PACKAGES}; do \
		go fmt $$pkg; \
	done;

test:
	go test ./...

gen_mock:
	mockery

golint:
	for pkg in ${PACKAGES}; do \
		golint -set_exit_status $$pkg || GOLINT_FAILED=1; \
	done; \
	[ -z "$$GOLINT_FAILED" ]
