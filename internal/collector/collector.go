package collector

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

// TODO: format
// TODO: version and build time
// TODO: use Go 1.16's ability to have this separate
const indexHtml = `
<!doctype html>

<html lang="en">
<head>
  <meta charset="utf-8">

  <title>Hoard</title>


 <link rel="preconnect" href="https://fonts.gstatic.com">
<link href="https://fonts.googleapis.com/css2?family=Alfa+Slab+One&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Roboto+Mono&display=swap" rel="stylesheet"> 
<link href="https://fonts.googleapis.com/css2?family=Roboto&family=Roboto+Mono&display=swap" rel="stylesheet"> 
<style>
body {
	font-family: Roboto, sans-serif;
}
h1 {
    font-family: 'Alfa Slab One', cursive;
font-size: 100px;
text-align: center;
margin-bottom: 0;
padding-bottom: 0;
}

.secret {

background: #ccc;
}

pre {
font-family: 'Roboto Mono', monospace;
font-size: 14px;
border: 1px solid gray;
padding: 10px;
width: 800px;
margin: 0 auto;
}

p {
     text-align: center;
}
</style>
</head>

<body>
<h1>HOARD</h1>
<p>Started %s ago &bull; <a href="./metrics">Prometheus metrics endpoint</a></p>
<p>Configuration for this replica:</p>
<pre>%s</pre>
<p>
Hoard is a distributed fault-tolerant application for collecting data feeds.
<a href="https://github.com/jamespfennell/hoard">Check it out on Github</a>.
</p>
</body>
</html>

`

var startTime = time.Now().UTC()

func Run(c *config.Config, interruptChan <-chan struct{}) error {
	// TODO: if there is an error here it should crash the program
	// TODO: graceful shutdown
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// TODO: pretty print the duration
		w.Write([]byte(fmt.Sprintf(indexHtml,
			time.Now().UTC().Sub(startTime).Truncate(time.Second),
			c)))
	})
	return http.ListenAndServe(fmt.Sprintf(":%d", c.Port), nil)
}
