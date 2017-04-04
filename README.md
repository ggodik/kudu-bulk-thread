## bulk loading / threading testing

George Godik ggodik@factset.com

### Compile

```scons```

Note: make sure to have kudu libs and scons installed


### Run

```
export KUDU_MASTER="master-url.fqdn"
./kudu-bulk-load 1000000 100 50 4
```

this will do 2 sets of insertions

 * first non-threaded attempt: insert 1M rows of 100 columns in batches of 20K into a table called _t_1000000_100_
 * second threaded attempt: insert 1M rows of 100 columns in batches of 20K into a table called _t_1000000_100_threaded using 4 threads


### Configure

The following env vars are avaiable

 * BUFFER_SIZE - (int in MB) - size of transaction buffer
 * KUDU_MASTER - (fqdn) master host to connect to
 * KUDU_TABLETS - (int) number of tablets for table paritions