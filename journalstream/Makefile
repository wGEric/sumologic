TARGET  ?= build

DOCKER ?= docker
DOCKERTAG ?= latest
DOCKERREPO ?= localhost
APPNAME = sumologic-journalstream
WORKDIR ?= $(CURDIR)
BUILDER ?= centurylink/golang-builder
TESTER ?= centurylink/golang-tester

build:
	$(DOCKER) pull $(BUILDER)
	$(DOCKER) run --rm \
		--privileged \
		-v $(WORKDIR):/src \
		-e CGO_ENABLED=1 \
		-e LDFLAGS='-s -extldflags -static' \
		-v /var/run/docker.sock:/var/run/docker.sock \
		$(BUILDER) \
		$(DOCKERREPO)/$(APPNAME):$(DOCKERTAG)
	$(DOCKER) tag -f $(DOCKERREPO)/$(APPNAME):$(DOCKERTAG) $(DOCKERREPO)/$(APPNAME):latest

test:
	$(DOCKER) pull $(TESTER)
	$(DOCKER) run --rm \
		--privileged \
		-v $(WORKDIR):/src \
		-v /var/log/journal:/var/log/journal:ro \
		-v /run/systemd/journal:/run/systemd/journal:rw \
		$(TESTER) \
		$(DOCKERREPO)/$(APPNAME):$(DOCKERTAG)

push:
	$(DOCKER) push $(DOCKERREPO)/$(APPNAME):$(DOCKERTAG)
	$(DOCKER) push $(DOCKERREPO)/$(APPNAME):latest

clean:
	rm -f $(APPNAME)
