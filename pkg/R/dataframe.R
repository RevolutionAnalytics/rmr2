do.call.dots = 
	function(what, ..., args, quote = FALSE, envir = parent.frame())
		do.call(what, c(list(...), args), quote = quote, envir = envir)

do.data.frame = 
	function(.data, f,  ..., named = TRUE,  envir = sys.frame(1)) {
		dotlist = {
			if(named)
				named_dots(...)
			else
				dots(...) }
		env = list2env(.data, parent = envir)
		dotvals = lapply(dotlist, function(x) eval(x, env))
		do.call.dots(f, .data, args = dotvals)}


subset3.data.frame = 
	function(.data, ...)
		do.data.frame(
			.data, 
			function(x, cond) x[cond, ], 
			...,
			named = FALSE)

#
(function(){x = 5; subset3.data.frame(mtcars, cyl>x)})()

select.data.frame =
	function(.data, ...)
		do.data.frame(
			.data, 
			function(x, ...) data.frame(...), 
			..)

#(function(){select.data.frame(mtcars, cyl, carb)})()

add.cols.data.frame  = 
	function(.data, ...)
		do.data.frame(.data, function(x, ...) cbind(x, data.frame(...)), ...)

#(function(){v = 4; add.cols.data.frame(mtcars, x = v)})()

map.data.frame = 
	function(.data, ...)
		do.data.frame(.data, function(x, ...) data.frame(...), ...)

 (function(){v = 5; map.data.frame(mtcars,  v + cyl)})()