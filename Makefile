clean_tests:
	@find tests/ -type f -name "*.data" -exec rm -f {} \;
	@find tests/ -type f -name "*.index" -exec rm -f {} \;
	@find tests/ -type f -name "*.txt" -exec rm -f {} \;
.PHONY: clean_tests
