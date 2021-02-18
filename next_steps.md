
- Update to Go 1.16
- Clean up the server code, rename it, extract HTML using go 1.16
- Built at time? And version?  


- Clean up the code added during the merging step
- Metrics for disk usage

- Think about versioning
- Use a context for timing out and shutting down

When logging is figured out just search for all Printf occurences

Rename a -> astore and d -> dstore
Change interface to have io.WriterCloser and io.ReaderCloser?
 - Will reduce the memory footprint
 - May be hard to do it efficiently for Archive though

%w fmt verb
