\name{make.input.format}
\alias{make.input.format}
\alias{make.output.format}

\title{
 Create combinations of settings for flexible IO
}
\description{
Create combinations of IO settings either from named formats or from a combination of a Java class, a mode and an R function
}
\usage{
make.input.format(format = make.native.input.format(), mode = c("binary", "text"), streaming.format = NULL, ...)
make.output.format(format = make.native.output.format( keyval.length =   rmr.options("keyval.length")), 
                   mode = c("binary", "text"), 
                   streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", ...)}

\arguments{
  \item{format}{Either a string describing a predefined combination of IO settings (possibilities include: \code{"text"}, \code{"json"}, \code{"csv"}, \code{"native"},\code{"sequence.typedbytes"}) or a function. For an input format, this function accepts a connection and a number of records and returns a key-value pair (see \code{\link{keyval}}). For an output format, this function accepts a key-value pair and a connection and  writes the former to the latter.}
  \item{mode}{Mode can be either \code{"text"} or \code{"binary"}, which tells R what type of connection to use when opening the IO connections.}
  \item{streaming.format}{Class to pass to hadoop streaming as \code{inputformat} or \code{outputformat} option. This class is the first in the input chain to perform its duties on the input side and the last on the output side. Right now this option is not honored in local mode.}
  \item{\dots}{Additional arguments to the format function. For the csv format they detail the specifics of the csv dialect to use and are the same as for \code{\link{read.table}} and \code{\link{write.table}} for the input and output resp, with the exception of \code{header, file, x, nrows, col.names} and \code{row.names}. For \code{"json"}, only on the input side, one can specify a \code{key.class} and a \code{value.class} to help in mapping the JSON data model to R's own more flexibly. For the \code{"native"} and \code{"sequence.typedbytes"} output formats the user can specify a \code{keyval.length} that says how many values to map to a single physical key-value pair when the key is \code{NULL}. For the \code{"hbase"} format, the table name is provided as the input argument to \code{\link{mapreduce}}. Additional arguments are: \code{family.columns}, a named list where the names are family names and the elements are lists of column names within each family; \code{key.deserialize} and \code{cell.deserialize} that control the deserialization of keys and cells resp. and can take a string value or a function (explained below); \code{dense} which contols whether the data read from hbase is returned as a 4-column data frame (key, family, column and cell) or a number of columns equal to the number of columns selected, plus one for the key; finally \code{atomic} which contols whether the data frame columns are atomic or returned "as is", see \code{\link{I}}. The allowed values for the deserialization argument are \code{"raw"}, which means cells are text; \code{"typdebytes"}, which is a serialization format shared with other elements of the Hadoop system; \code{"native"} which is the native R format; or a function that takes a list of raw vectors and returns a list or vector of deserialized objects. In the case of cell.deserialize the function should take two additional argmuments for the names of family and column being deserialized }}

\details{
The goal of these functions is to encapsulate some of the complexity of the IO settings, providing meaningful defaults and predefined combinations.  If you don't want to deal with the full complexity of defining custom IO formats, there are prepackaged combinations. "text" is free text, useful mostly on the input side for NLP type applications; "json" is one or two tab separated, single line JSON objects per record; "csv" is the CSV format, configurable through additional arguments; "native.text" uses the internal R serialization in text mode, and was the default in previous releases, use only for backward compatibility; "native" uses the internal R serialization, offers the highest level of compatibility with R data types and is the default; "sequence.typedbytes" is a sequence file (in the Hadoop sense) where key and value are of type typedbytes, which is a simple serialization format used in connection with streaming for compatibility with other hadoop subsystems. Typedbytes is documented here \url{https://hadoop.apache.org/mapreduce/docs/current/api/org/apache/hadoop/typedbytes/package-summary.html}. "hbase" allows to read from (but not yet write to) an HBase table. This format should still considered experimental. Hadoop should be already configured to run streaming jobs on HBase tables \url{https://wiki.apache.org/hadoop/Hbase/MapReduce}.
If you want to implement custom formats, the input processing is the result of the composition of a Java class and an R function, and the same is true on the output side but in reverse order and you can specify both as arguments to this functions.}

\value{
Return a list of IO specifications, to be passed as \code{input.format} and \code{output.format} to \code{\link{mapreduce}}, and as \code{format} to  \code{\link{from.dfs}} (input) and  \code{\link{to.dfs}} (output).}

\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.
make.input.format("csv", sep = ",")
}