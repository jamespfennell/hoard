# Hoard

**hoard** _v._ Accumulate (money or valued objects) and hide or store away.

Hoard is an application for collecting data feeds over time to create a data lake. 
For each feed of interest, Hoard downloads the feed periodically (typically every few seconds), 
bundles the results for each hour into archive files, 
and then stores these archive files in object storage for later retrieval. 
The application was originally developed to collect New York City subway data, 
and is optimized for feeds whose periodicity is between hundreds of milliseconds to minutes.

One of the key features of Hoard is that it can run with multiple replicas, 
each tracking the same data feeds and contributing to the same archive files in bucket storage. 
This makes the data collection process resilient to downtime on individual nodes running Hoard, 
and ensures the resulting data lake is as complete as possible.
