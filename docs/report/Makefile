PDF = /usr/local/bin/dvipdf
TEX = /usr/texbin/pdflatex
BIB = /usr/texbin/bibtex
RM = /bin/rm
CP = /bin/cp
MV = /bin/mv

all: report.pdf

report.aux: report.tex ../refs/references.bib
	$(CP) ../refs/references.bib references.bib
	$(TEX) report.tex -output-directory=../output
	$(BIB) report.aux
	$(RM) references.bib

report.pdf: report.aux
	$(TEX) report.tex
	$(TEX) report.tex
	$(MV) report.pdf ../output/
	
clean:
	$(RM) -f *{out,bbl,blg,dvi,log,aux,hst,ver,toc,lof,lot,upa,upb}
