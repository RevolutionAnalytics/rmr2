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
make.input.format(format = "native", mode = c("binary", "text"), streaming.format = NULL, backend.parameters = NULL, ...)
make.output.format(format = "native", mode = c("binary", "text"), streaming.format = NULL, backend.parameters = NULL, ...)}

\arguments{
  \item{format}{Either a string describing a predefined combination of IO settings (possibilities include: \code{"text"}, \code{"json"}, \code{"csv"}, \code{"native"},\code{"sequence.typedbytes"}, \code{"hbase"}, \code{"pig.hive"}) or a function. For an input format, this function accepts a connection and a number of records and returns a key-value pair (see \code{\link{keyval}}). For an output format, this function accepts a key-value pair and a connection and  writes the former to the latter.}
  \item{mode}{Mode can be either \code{"text"} or \code{"binary"}, which tells R what type of connection to use when opening the IO connections.}
  \item{streaming.format}{Class to pass to hadoop streaming as \code{inputformat} or \code{outputformat} option. This class is the first in the input chain to perform its duties on the input side and the last on the output side. Right now this option is not honored in local mode.}
  \item{backend.parameters}{Sometimes the class passed to hadoop streaming (see the \code{streaming.format} argument) needs additional information to be properly configured which can be supplied to the streaming command line, typically with the \code{-D} option. This information can be specified with thi argument, with a named list where the names are the option name, typically \code{"D"}, and the values are \code{"property=value"} pairs as length-one character vectors, as in \code{backend.parameters = list(D="property1=value1", D="property2=value2")} }
  \item{\dots}{Additional arguments to the format function. }
  All existing input formats accept a \code{read.size} (for binary) or \code{nrow} (for text) argument representing a number of bytes or number of rows to parse for each map call. These settings are used as suggestions and can not be honored exactly. Correctness of a program should not depend on their values. Custom formats created by users do not need to accept these arguments, but need to provide some other mechanism to read from the input connection in reasonably sized chunk. Moving to more format-specific arguments:
  \describe{
  \item{csv}{Arguments are the same as for \code{\link{read.table}} and \code{\link{write.table}} for the input and output resp, with the exception of \code{header, file, x, col.names} and \code{row.names}, the latter two allowed for input only.}
  \item{json}{only on the input side, \code{key.class} and a \code{value.class} help in mapping the JSON data model to R's own more flexibly. An attempt wil be made to cast the data to the specified classes.}
  \item{native, sequence.typedbytes}{\code{write.size} says how many values to map to a single typedbytes key-value pair when the key is \code{NULL}. }
  \item{hbase}{The name of the table to read from is provided as the input argument to \code{\link{mapreduce}}. Additional arguments are: 
  \describe{
  \item{\code{family.columns}}{a named list where the names are family names and the elements are lists of column names within each family;} 
  \item{\code{key.deserialize} and \code{cell.deserialize}}{control the deserialization of keys and cells resp. and can take a string value or a function (explained below); allowed values are \code{"raw"}, which means cells are text; \code{"typdebytes"}, which is a serialization format shared with other elements of the Hadoop system; \code{"native"} which is the native R format; or a function that takes a list of raw vectors and returns a list or vector of deserialized objects. In the case of \code{cell.deserialize} the function should take two additional argmuments for the names of family and column being deserialized} 
  \item{\code{dense}}{contols whether the data read from hbase is returned as a 4-column data frame (key, family, column and cell) or a number of columns equal to the number of columns selected, plus one for the key;} 
  \item{\code{atomic}}{contols whether the data frame columns are atomic or returned "as is", see \code{\link{I}}.}
  \item{\code{start.row} and \code{stop.row}}{limit input to a range of keys between the two supplied arguments, each optional; remeber the order is based on the byte representation of the keys}
  \item{\code{row.filter}}{specify a regex to filter the input table}}}}}

\details{
The goal of these functions is to encapsulate some of the complexity of the IO settings, providing meaningful defaults and predefined combinations.  If you don't want to deal with the full complexity of defining custom IO formats, there are prepackaged combinations. 
\describe{
\item{text}{free text, useful mostly on the input side for NLP type applications;}
\item{json}{one or two tab separated, single line JSON objects per record;}
\item{csv}{is the CSV format, configurable through additional arguments;}
\item{native}{uses the internal R serialization, offers the highest level of compatibility with R data types and is the default;}
\item{sequence.typedbytes}{is a sequence file (in the Hadoop sense) where key and value are of type typedbytes, which is a simple serialization format used in connection with streaming for compatibility with other hadoop subsystems. Typedbytes is documented here \url{https://hadoop.apache.org/mapreduce/docs/current/api/org/apache/hadoop/typedbytes/package-summary.html}}
\item{hbase}{allows to read from (but not yet write to) an HBase table. This format should still considered experimental. Hadoop should be already configured to run streaming jobs on HBase tables \url{https://wiki.apache.org/hadoop/Hbase/MapReduce}.}
\item{pig.hive}{ is a variant of CSV to tranfer data to and from Hive or Pig, when using their default format \code{`ROW FORMAT DELIMITED FIELDS TERMINATED BY '001' LINES TERMINATED BY '\n'`}.}}
If you want to implement custom formats, the input processing is the result of the composition of a Java class and an R function, and the same is true on the output side but in reverse order and you can specify both as arguments to these functions.}

\value{
Return a list of IO specifications, to be passed as \code{input.format} and \code{output.format} to \code{\link{mapreduce}}, and as \code{format} to  \code{\link{from.dfs}} (input) and  \code{\link{to.dfs}} (output).}

\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.
make.input.format("csv", sep = ",")
}