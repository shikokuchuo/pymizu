# Cross-language round-trip, R host side: a Python peer spawned through
# `python -m pymizu.child`, driven by a source-string drop (the R child's
# parse+eval path is exercised by the reverse direction). argv: the python
# executable. Exits nonzero on any mismatch; the pytest side asserts.

args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 1L)
py <- args[[1L]]
library(mizu)

peer_src <- "
import pymizu

while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x)
"

launcher <- function(token) {
  system2(py, c("-m", "pymizu.child", token), wait = FALSE)
}

ch <- mizu_channel(peer_src, launcher = launcher)

# small numeric: RAWVEC both ways
x <- c(1.5, 2.5, 3.5)
mizu_send(ch, x)
stopifnot(identical(mizu_recv(ch, timeout = 30), x))

# large numeric: SHM_VEC both ways — a zero-copy read-only view on the
# Python side, and the echoed view re-stages as a fresh SHM_VEC region
# (+ 0 materializes: an ALTREP sequence would serialize, never SHM_VEC)
big <- as.numeric(seq_len(1000000)) + 0
mizu_send(ch, big)
stopifnot(identical(mizu_recv(ch, timeout = 60), big))

# integer and raw
i <- seq_len(100L) + 0L
mizu_send(ch, i)
stopifnot(identical(mizu_recv(ch, timeout = 30), i))
r <- as.raw(0:255)
mizu_send(ch, r)
stopifnot(identical(mizu_recv(ch, timeout = 30), r))

# a string echoes too: a Python str crosses as STR1, the mirror of R's
# length-1 string tier
s <- "hello"
mizu_send(ch, s)
stopifnot(identical(mizu_recv(ch, timeout = 30), s))

stopifnot(mizu_close(ch, timeout = 10))
cat("OK\n")
