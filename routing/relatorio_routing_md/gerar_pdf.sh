#!/bin/sh
# Gera o PDF a partir do Markdown (precisa de pandoc e xelatex)
pandoc relatorio_routing.md -o relatorio_routing.pdf --pdf-engine=xelatex \
  -V geometry:margin=2.2cm -V mainfont="DejaVu Serif" -V sansfont="DejaVu Sans" \
  -V monofont="DejaVu Sans Mono" -V fontsize=10pt -V colorlinks=true -V linkcolor=blue \
  -H header.tex
