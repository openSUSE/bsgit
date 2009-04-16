NAME=bsgit
VERSION=$(shell cat VERSION)

FILES := COPYING bsgit.py bsgit/__init__.py bsgit/bscache.py setup.py
all:

bsgit.spec: bsgit.spec.in VERSION
	sed -e 's:@VERSION@:$(VERSION):g' $< > $@

dist: bsgit.spec
	@rm -f $(NAME)-$(VERSION)
	ln -s . $(NAME)-$(VERSION)
	tar cf - $(FILES:%=$(NAME)-$(VERSION)/%) \
	    | gzip -9 > $(NAME)-$(VERSION).tar.gz
	rm -f $(NAME)-$(VERSION)
