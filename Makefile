.DEFAULT: build

.PHONY: build
build: setup.py sbalance/sbalance.py
	python setup.py sdist bdist_wheel

.PHONY: dist
dist: 
	twine upload dist/*

.PHONY: test-dist
test-dist:
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

.PHONY: clean
clean:
	rm -r dist
	rm -r build
	rm -r *.egg-info
