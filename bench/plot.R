arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
here <- dirname(sub("^--file=", "", arg))
d <- read.csv(file.path(here, "results.csv"))
d$rate <- d$delivered / d$seconds
d$datagrams_per <- d$datagrams / d$delivered

# Both broadcasts against column `x` of one experiment.
figure <- function(file, experiment, x, xlab, legend_at) {
  e <- d[d$experiment == experiment & startsWith(d$program, "broadcast"), ]
  s <- aggregate(e[c("rate", "datagrams_per")], list(program = e$program, x = e[[x]]), median)
  layered <- s[s$program == "broadcast", ]
  fused <- s[s$program == "broadcast --fused", ]

  png(file.path(here, file), width = 1200, height = 520, res = 144)
  par(mfrow = c(1, 2), las = 1, mar = c(4.5, 5, 2.5, 1))
  plot(layered$x, layered$rate / 1000, type = "b", log = "y", yaxt = "n",
       ylim = range(s$rate / 1000), xlab = xlab, ylab = "",
       main = "Thousand messages per second")
  axis(2, at = c(20, 50, 100, 200, 500, 1000, 2000))
  lines(fused$x, fused$rate / 1000, type = "b", pch = 2, lty = 2)
  legend(legend_at, c("layered", "single layer"), pch = 1:2, lty = 1:2, bty = "n")
  plot(layered$x, layered$datagrams_per, type = "b",
       ylim = c(0, max(s$datagrams_per)), xlab = xlab, ylab = "",
       main = "Datagrams sent per message")
  lines(fused$x, fused$datagrams_per, type = "b", pch = 2, lty = 2)
  invisible(dev.off())
}
figure("broadcast.png", "scaling", "processes", "processes", "topright")
figure("loss.png", "loss", "loss", "fraction of datagrams lost", "bottomleft")

t <- aggregate(rate ~ program + loss, d[d$experiment == "loss", ], median)
cat("| | no loss | 20% loss |\n|---|---:|---:|\n")
for (p in c("links", "broadcast", "broadcast --fused", "lattice")) {
  r <- t[t$program == p, ]
  rate <- function(loss) format(signif(r$rate[r$loss == loss], 3), big.mark = ",", scientific = FALSE)
  unit <- if (p == "lattice") "instances/s" else "messages/s"
  cat(sprintf("| `%s`, %s | %s | %s |\n", p, unit, rate(0), rate(0.2)))
}
