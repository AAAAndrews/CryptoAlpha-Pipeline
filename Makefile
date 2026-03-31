SHELL := /bin/bash

.PHONY: init-plan run-agent validate

init-plan:
	cmd /c 1-init-planner.bat

run-agent:
	cmd /c 2-run-coder.bat

validate:
	test -f 1-init-planner.bat
	test -f 2-run-coder.bat
	python3 -m json.tool scarffold/.agent/tasks.json >/dev/null
	python3 -m json.tool scarffold/.agent/tasks.schema.json >/dev/null
	@echo "Validation passed"
