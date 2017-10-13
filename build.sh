#!/bin/bash

msg() {
    printf "\033[1;32m :: %s\n\033[0m" "$1"
}

msg "Building jar"
Rscript compile.r

msg "Documenting R code"
R -e 'devtools::document()'

msg "Updating R bindings"
R -e 'devtools::install()'
