\name{dfs.empty}
\alias{dfs.empty}
\alias{dfs.exists}
\alias{dfs.size}
\alias{dfs.mv}
\alias{dfs.rmr}
\alias{dfs.ls}
\title{Backend-independent file manipulation}

\description{Check if an item is empty or return its size. Move an item or remove(recursively). Here item is a valid path or \code{\link{big.data.object}}}
 
\usage{
dfs.empty(fname)
dfs.exists(fname)
dfs.size(fname)
dfs.mv(from, to)
dfs.rmr(fname)
dfs.ls(fname)
}

\arguments{
  \item{fname}{A valid path or \code{\link{big.data.object}}}
  \item{from, to}{A valid path}
}

\value{For \code{dfs.size} a number of bytes; for \code{dfs.empty} and \code{dfs.exists}, a logical; for \code{dfs.ls} a data.frame}

\details{
The size of a directory, for the sake of this commands, is the size of the files contained therein with the exception of hidden files starting with "." and "_". This is not well documented in Hadoop but there is a private call that implements this pattern.  }

\examples{
dfs.empty(mapreduce(to.dfs(1:10)))
dfs.size(mapreduce(to.dfs(1:10)))
}