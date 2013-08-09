\name{rmr.options}
\alias{rmr.options}
\title{Function to set and get package options}
\description{Set and get package options}
\usage{
rmr.options(backend = c("hadoop", "local"), profile.nodes = c("off", "calls", "memory", "both"), keyval.length = 10^4, dfs.tempdir = NULL)
}
\arguments{
  \item{...}{Names of options to get values of, as character}
  \item{backend}{One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially, for learning and debugging.}
  \item{profile.nodes}{Collect profiling and memory information when running additional R interpreters (besides the current one) on the cluster. No effect on the local backend, use Rprof instead. For backward compatibility, \code{"calls"} is equivalent to \code{TRUE} and \code{"off"} to \code{FALSE}}
  \item{keyval.length}{Min number of records to read into a single, key-value pair or how many to write to when the key is \code{NULL}. Controls both \code{\link{from.dfs}} and \code{\link{to.dfs}} and \code{\link{mapreduce}} behavior. This setting will be ignored at the end of file or partitions, when not enough records are avilable}
  \item{dfs.tempdir}{The directory to use for temporary files, including \code{\link{mapreduce}} intermediate results files, on the distributed file system.}
}
\details{
 Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to mapreduce through the hadoop backend and
provide an all-R, single interpreter implementation (local) that's good for experimentation and debugging, in particular to debug mappers and
reducers. Profiling data is collected in the following files: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapreduce_tip_id'))} on each node. The path is printed in stderr for your convenience and you will find in in the logs, specifically stderr for each attempt. Then you need to ssh to the machine where that attempt run to examine or retrieve it. \code{keyval.length} is used as a hint, particularly as a lower bound hint for how many records are actually processed by each map call. 
}
\value{A named list with the options and their values, or just a value if only one requested. NULL when only setting options.}


\examples{
old.backend = rmr.options("backend")
rmr.options(backend = "hadoop")
rmr.options(backend = old.backend)
rmr.options(
  dfs.tempdir = 
    file.path(
      "/user", 
      system("whoami", TRUE),
      "tmp-rmr2", 
      basename(tempdir())))
}
 