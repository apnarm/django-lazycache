$(eval VERSION := $(shell python setup.py --version))
SDIST := dist/django-lazycache-$(VERSION).tar.gz

all: build

build: $(SDIST)

$(SDIST):
	python setup.py sdist
	rm -rf *.egg *.egg-info

.PHONY: install
install: $(SDIST)
	sudo pip install $(SDIST)

.PHONY: uninstall
uninstall:
	sudo pip uninstall django-lazycache

.PHONY: register
register:
	python setup.py register
	clean
	rm -rf *.egg *.egg-info

.PHONY: upload
upload:
	python setup.py sdist upload
	rm -rf *.egg *.egg-info

.PHONY: clean
clean:
	rm -rf dist *.egg *.egg-info
