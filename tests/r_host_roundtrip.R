# Cross-language round-trip, R host side: a Python peer spawned through
# `python -m pyrei.child`, driven by a source-string drop (the R child's
# parse+eval path is exercised by the reverse direction). argv: the python
# executable. Exits nonzero on any mismatch; the pytest side asserts.

args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 1L)
py <- args[[1L]]
library(rei)

peer_src <- "
import pyrei

while True:
    x = ch.recv(30)
    if pyrei.is_sentinel(x):
        break
    ch.send(x)
"

launcher <- function(token) {
  system2(py, c("-m", "pyrei.child", token), wait = FALSE)
}

ch <- rei_channel(peer_src, launcher = launcher)

# small numeric: RAWVEC both ways
x <- c(1.5, 2.5, 3.5)
rei_send(ch, x)
stopifnot(identical(rei_recv(ch, timeout = 30), x))

# large numeric: SHM_VEC both ways — a zero-copy read-only view on the
# Python side, and the echoed view re-stages as a fresh SHM_VEC region
# (+ 0 materializes: an ALTREP sequence would serialize, never SHM_VEC)
big <- as.numeric(seq_len(1000000)) + 0
rei_send(ch, big)
stopifnot(identical(rei_recv(ch, timeout = 60), big))

# integer and raw
i <- seq_len(100L) + 0L
rei_send(ch, i)
stopifnot(identical(rei_recv(ch, timeout = 30), i))
r <- as.raw(0:255)
rei_send(ch, r)
stopifnot(identical(rei_recv(ch, timeout = 30), r))

# a string echoes too: a Python str crosses as STR1, the mirror of R's
# length-1 string tier
s <- "hello"
rei_send(ch, s)
stopifnot(identical(rei_recv(ch, timeout = 30), s))

stopifnot(rei_close(ch, timeout = 10))
cat("OK\n")
