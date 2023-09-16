test:
	. ./.secret.sh && \
		$(VENV)/python ./aqicn.py current --dry-run
run:
	. ./.secret.sh && \
		$(VENV)/python ./aqicn.py current --random-sleep 0

include Makefile.venv
