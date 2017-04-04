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

### Output

```
fdscloud@CPEDev071:~/kudu-bulk-thread$ ./kudu-bulk-load 1000000 100 50 4
Running with Kudu client version: kudu 1.2.0-cdh5.10.1 (rev 68a3a5f390628db0ff9a15968d302102fa17e86a)
Long version info: kudu 1.2.0-cdh5.10.1
revision 68a3a5f390628db0ff9a15968d302102fa17e86a
build type RELEASE
built by None at 06 Mar 2017 12:44:47 PST on ec2-pkg-ubuntu-16-04-impala-0b9d.vpc.cloudera.com
Host:cpedev091.sandbox.factset.com Tablets:7 BUFFER_SIZE:15000000
Created a client connection
Created a schema
Deleting old table before creating new one
Create Table took:0.004228s
Create Table took:0.003711s
Created a tables:t_1000000_102_threaded and t_1000000_102
        START::Inserting rows:0 to 1000000 flushing:20000
	DONE::Insert Rows:0 took:6.15624s
Non-Threaded insert took:6.15758s
	START::Inserting rows:0 to 250000 flushing:20000
	START::Inserting rows:500000 to 750000 flushing:250000
	START::Inserting rows:750000 to 1000000 flushing:20833
	START::Inserting rows:250000 to 500000 flushing:20833
	DONE::Insert Rows:2 took:6.2999s
	DONE::Insert Rows:0 took:6.40102s
	DONE::Insert Rows:3 took:6.59889s
	DONE::Insert Rows:1 took:6.6007s
Threaded insert:4 took:6.61822s
Done
										
```