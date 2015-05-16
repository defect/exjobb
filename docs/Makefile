DIRS = report

PDF = /usr/local/bin/dvipdf
TEX = /usr/texbin/latex

all: report.pdf

report.pdf:
	$(MAKE) -C report

clean:
	-for d in $(DIRS); do ($(MAKE) -C $$d clean ); done
