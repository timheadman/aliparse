ARGS=$(filter-out $@,$(MAKECMDGOALS))
BRANCH=`git rev-parse --abbrev-ref HEAD`
ENV=`basename "$PWD"`

.PHONY: clean
# target: clean - Clean temporary files
clean:
	@find . -name "*.py[co]" -delete
	@find . -name "*.orig" -delete
	@find . -name "*.deb" -delete
	@rm -rf build dist

.PHONY: shell
# target: shell - Run project shell
shell:
	@python manage.py shell_plus --ipython || python manage.py shell

.PHONY: black
# target: black - Fix python styling code
black:
	@black . --skip-string-normalization

.PHONY: isort
# target: black - Fix python styling code
isort:
	@isort .
