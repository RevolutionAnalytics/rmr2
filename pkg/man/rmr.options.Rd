\name{rmr.options}
\alias{rmr.options}
\title{Function to set and get package options}
\description{Set and get package options}
\usage{
rmr.options(
  backend = c("hadoop", "local"), 
  profile.nodes = c("off", "calls", "memory", "both"), 
  dfs.tempdir = NULL,
  exclude.objects = NULL,                 
  backend.parameters = list())
}
\arguments{
  \item{...}{Names of options to get values of, as length one character vectors}
  \item{backend}{One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially, for learning and debugging.}
  \item{profile.nodes}{Collect profiling and memory information when running additional R interpreters (besides the current one) on the cluster. No effect on the local backend, use Rprof instead. For backward compatibility, \code{"calls"} is equivalent to \code{TRUE} and \code{"off"} to \code{FALSE}}
  \item{dfs.tempdir}{The directory to use for temporary files, including \code{\link{mapreduce}} intermediate results files, on the distributed file system.}
  \item{exclude.objects}{Objects in the Global environment that are not needed by the map or reduce functions, as character vector}
  \item{backend.parameters}{Parameters to pass directly to the backend. See equally named argument for the function \code{\link{mapreduce}}. Use this setting for backend parameters that need to be different from default but can be the same from job to job}
}
\details{
 While the main goal for rmr2 is to provide access to hadoop mapreduce, the package has a notion of a backend that can be swapped while preserving most features. One backend is of course hadoop itself, the other is called "local" and is implemented within the current interpreter and using the local file system. rmr2 programs run on the local backend are ordinary (non-distributed, single-threaded) programs which is particularly useful for learning and debugging (debug, recover and trace work). Profiling data is collected in the following files: \code{file.path(rmr.options("dfs.tempdir"), "Rprof", <job id>, <attempt id>)} on each node (the details of how job id and attempt id are obtained depend upon the Hadoop distribution) The path is printed in stderr for your convenience and you will find in in the logs, specifically stderr, for each attempt. Then you need to ssh to the machine where that attempt run to examine or retrieve it. \code{keyval.length} is used as a hint, particularly as a lower bound hint for how many records are actually processed by each map call. 
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
 