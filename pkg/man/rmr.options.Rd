\name{rmr.options}
\alias{rmr.options}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Function to set and get package options}
\description{
set and get package options}
\usage{
rmr.options(backend = c("hadoop", "local"), profile.nodes = c("off", "calls", "memory", "both"), keyval.length = 10000, read.size = 10^7, dfs.tempdir = tempdir(), rscript.cmd = 'Rscript')
% depend.check = NULL, managed.dir = NULL)
}
\arguments{
  \item{...}{Names of options to get values of, as character}
  \item{backend}{One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially, for learning and debugging.}
  \item{profile.nodes}{Collect profiling and memory information when running additional R interpreters (besides the current one) on the cluster. No effect on the local backend, use Rprof instead. For backward compatibility, \code{"calls"} is equivalent to \code{TRUE} and \code{"off"} to \code{FALSE}}
  \item{keyval.length}{Min number of records to read into a single, key-value pair or how many to write to when the key is \code{NULL}. Controls both \code{\link{from.dfs}} and \code{\link{to.dfs}} and \code{\link{mapreduce}} behavior. Works in combination with the \code{read.size} option. This setting will be ignored at the end of file or partitions, when not enough records are avilable}
  \item{read.size}{Initial number of bytes to read when parsing binary formats (including the default, \code{native} format). If the number of parsed records is different than \code{keyval.length}, the effective read size get adjusted. All parsed records are passed to the user code, so specifically this option and \code{keyval.length} should not be used to precisely partition the data but only to optimize performance and comply with memory limitations. Correctness should be independent of these settings}
  \item{dfs.tempdir}{The directory to use for temporary files, including \code{\link{mapreduce}} intermediate results files, on the distributed file system.}
  \item{rscript.cmd}{The command used to invoke R on the nodes, allows to specify additional options, it should start with 'Rscript' unless you absolutely know what you are doing}
 % \item{depend.check}{Activate makefile-like dependency checking (under construction)}
 % \item{managed.dir}{Where to put intermediate result when makefile-like features are activated}
}
\details{
 Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to mapreduce through the hadoop backend and
provide an all-R, single interpreter implementation (local) that's good for experimentation and debugging, in particular to debug mappers and
reducers. Profiling data is collected in the following files: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapreduce_tip_id'))} on each node. The path is printed in stderr for your convenience and you will find in in the logs, specifically stderr for each attempt. Then you need to ssh to the machine where that attempt run to examine or retrieve it. \code{keyval.length} is used as a hint, particularly as a lower bound hint for how many records are actually processed by each map call. 
%Describe dependency checking here 
}
\value{A named list with the options and their values, or just a value if only one requested. NULL when only setting options.}


\examples{
old.backend = rmr.options("backend")
rmr.options(backend = "hadoop")
rmr.options(backend = old.backend)
}
 