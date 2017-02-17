export GOPATH=$(CURDIR)/.go

APP_NAME = river
DEBIAN_TMP = $(CURDIR)/deb
VERSION = `$(CURDIR)/out/$(APP_NAME) -v | cut -d ' ' -f 3`

$(CURDIR)/out/$(APP_NAME): $(CURDIR)/src/main.go
	go build -o $(CURDIR)/out/$(APP_NAME) $(CURDIR)/src/main.go

dep-install:
	go get github.com/codegangsta/cli
	go get github.com/endeveit/go-snippets/config
	go get github.com/gemnasium/logrus-graylog-hook
	go get github.com/robfig/config
	go get github.com/sevlyar/go-daemon
	go get github.com/Sirupsen/logrus
	go get labix.org/v2/mgo
	go get gopkg.in/olivere/elastic.v5

git-magic:
	git submodule update --init --remote --recursive

fmt:
	gofmt -s=true -w $(CURDIR)/src

run:
	go run $(CURDIR)/src/main.go -b -c=$(CURDIR)/data/config.cfg
	
run-dev:
	go run --race $(CURDIR)/src/main.go -b -c=$(CURDIR)/data/config-dev.cfg

keys:
	openssl genrsa -out $(CURDIR)/out/rsakey 512
	openssl rsa -in $(CURDIR)/out/rsakey -pubout > $(CURDIR)/out/rsakey.pub

strip: $(CURDIR)/out/$(APP_NAME)
	strip $(CURDIR)/out/$(APP_NAME)

deb: $(CURDIR)/out/$(APP_NAME)
	mkdir $(DEBIAN_TMP)
	mkdir -p $(DEBIAN_TMP)/etc/$(APP_NAME)
	mkdir -p $(DEBIAN_TMP)/usr/local/bin
	install -m 644 $(CURDIR)/data/config.cfg $(DEBIAN_TMP)/etc/$(APP_NAME)
	install -m 755 $(CURDIR)/out/$(APP_NAME) $(DEBIAN_TMP)/usr/local/bin
	fpm -n $(APP_NAME) \
		-v $(VERSION) \
		-t deb \
		-s dir \
		-C $(DEBIAN_TMP) \
		--config-files   /etc/$(APP_NAME) \
		--after-install  $(CURDIR)/debian/postinst \
		--before-install $(CURDIR)/debian/preinst \
		--after-remove   $(CURDIR)/debian/postrm \
		--deb-init	     $(CURDIR)/debian/$(APP_NAME) \
		.
	rm -fr $(DEBIAN_TMP)

clean:
	rm -f $(CURDIR)/out/*

clean-deb:
	rm -fr $(DEBIAN_TMP)
	rm -f $(CURDIR)/*.deb

debug:
	echo $(GOPATH)
