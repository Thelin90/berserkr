.PHONY: deps
dependencies:
	SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv sync --dev --python 3.7

.PHONY: clean
clean:
	-find . -type f -name "*.pyc" -delete
	-find . -type f -name "*,cover" -delete