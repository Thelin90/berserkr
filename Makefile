include src/.env

.PHONY: clean
clean:
	-find . -type f -name "*.pyc" -delete
	-find . -type f -name "*,cover" -delete

.PHONY: test
test:
	PYTHONPATH=. pytest . -v

.PHONY: onlineretailer
onlineretailer:
	PYTHONPATH=. spark-submit --packages $(PYSPARK_SUBMIT_ARGS) --conf $(PYSPARK_CONF_ARGS) src/run_onlineretailer.py