test:
	. ./.secret.sh && \
		$(VENV)/python ./aqicn.py current --dry-run

include Makefile.venv
