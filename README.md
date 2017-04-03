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

this will attempt to insert 1M rows of 100 columns in batches of 20K using 4 threads