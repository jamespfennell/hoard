


- Clean up the code added during the merging step
- Metrics for disk usage

When logging is figured out just search for all Printf occurences
stop using ioutil
Rename a -> astore and d -> dstore
Change interface to have io.WriterCloser and io.ReaderCloser?
 - Will reduce the memory footprint
 - May be hard to do it efficiently for Archive though

%w fmt verb
