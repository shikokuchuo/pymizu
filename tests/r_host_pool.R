# Cross-language pool, R host side: Python workers spawned through
# `python -m pymizu.worker`, the R side the foreign submitter — the Python
# worker's foreign publish paths (the result policy, the err stream, its
# escape discipline, the map runner's element index) are asserted here.
# argv: the python executable. Exits nonzero on any mismatch; the pytest
# side asserts.

args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 1L)
py <- args[[1L]]
library(mizu)

p <- mizu_pool(
  2L,
  launcher = mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
)

# result values per the interchange table
t1 <- mizu_submit_call(p, mizu_call("numpy.mean", c(1, 2, 3, 4)))
stopifnot(identical(mizu_collect(t1), 2.5))
t2 <- mizu_submit_call(p, mizu_call(.source = "[1.5, 'two', None]"))
stopifnot(identical(mizu_collect(t2), list(1.5, "two", NULL)))

# a large result stages by the foreign result policy: values intact
t3 <- mizu_submit_call(
  p,
  mizu_call(.source = "import numpy as np\nnp.arange(100000.0)")
)
r3 <- mizu_collect(t3)
stopifnot(length(r3) == 100000L, r3[[1L]] == 0, r3[[100000L]] == 99999)

# a task error crosses as the err stream
e1 <- mizu_submit_call(p, mizu_call("builtins.len", 1.5))
c1 <- tryCatch(mizu_collect(e1), mizu_error_remote = function(c) c)
stopifnot(
  inherits(c1, "mizu_error_remote"),
  identical(c1$remote_type, "TypeError")
)

# a lone surrogate in the message crosses backslash-escaped, never blanked
e2 <- mizu_submit_call(
  p,
  mizu_call(.source = "raise ValueError('bad \\udcff path')")
)
c2 <- tryCatch(mizu_collect(e2), mizu_error_remote = function(c) c)
stopifnot(
  inherits(c2, "mizu_error_remote"),
  grepl("bad ", c2$message, fixed = TRUE)
)

# a non-portable result recovers as the task's err stream
e3 <- mizu_submit_call(p, mizu_call(.source = "{1, 2, 3}"))
c3 <- tryCatch(mizu_collect(e3), mizu_error_remote = function(c) c)
stopifnot(
  inherits(c3, "mizu_error_remote"),
  identical(c3$remote_type, "DeclinedError")
)

# a spec map's results land in order; a runner's error carries its
# element index (1-based on the R side)
r <- mizu_collect(mizu_map(p, 1:5, mizu_call(.source = "x * 2")))
stopifnot(identical(unlist(r), (1:5) * 2L))
src <- "if x == 5:\n    raise ValueError('elt')"
c4 <- tryCatch(
  mizu_collect(mizu_map(p, 1:10, mizu_call(.source = src))),
  mizu_error_remote = function(c) c
)
stopifnot(inherits(c4, "mizu_error_remote"), identical(c4[["index"]], 5L))

mizu_pool_stop(p)
cat("OK\n")
