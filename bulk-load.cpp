#include <ctime>
#include <iostream>
#include <sstream>
#include <thread>


#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/common/partial_row.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduStatusFunctionCallback;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;

using std::ostringstream;
using std::string;
using std::vector;

#include "util.h"

namespace
{
  const string master_host = std::getenv("KUDU_MASTER");
  const int BUFFER_SIZE = 15 * 1000000;
  const int NUMBER_OF_TABLETS = 9;
}

static Status CreateClient(const string& addr,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .add_master_server_addr(addr)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

static KuduSchema CreateSchema(const int cols) {
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)
    ->NotNull()
    ->Encoding(kudu::client::KuduColumnStorageAttributes::BIT_SHUFFLE)
    ->Compression(kudu::client::KuduColumnStorageAttributes::LZ4)
    ->PrimaryKey();

  for(int i = 0; i < cols; ++i)
    {
      b.AddColumn("int_"  + Grid::to_string(i))
	->Type(KuduColumnSchema::INT32)
	->Encoding(kudu::client::KuduColumnStorageAttributes::BIT_SHUFFLE)
	->Compression(kudu::client::KuduColumnStorageAttributes::LZ4)
	->NotNull();
    }
  KUDU_CHECK_OK(b.Build(&schema));
  return schema;
}

static Status DoesTableExist(const shared_ptr<KuduClient>& client,
                             const string& table_name,
                             bool *exists) {
  shared_ptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

static Status CreateTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema,
                          int num_tablets,
			  int num_rows) {
  Grid::Timer t("Create Table");
  vector<string> column_names;
  column_names.push_back("key");

  // Generate the split keys for the table.
  vector<const KuduPartialRow*> splits;
  int32_t increment = num_rows / num_tablets;
  for (int32_t i = 1; i < num_tablets; i++) {
    KuduPartialRow* row = schema.NewRow();
    KUDU_CHECK_OK(row->SetInt32(0, i * increment));
    splits.push_back(row);
  }
  
  // Create the table.
  KuduTableCreator* table_creator = client->NewTableCreator();
  Status s = table_creator->table_name(table_name)
    .schema(&schema)
    .add_hash_partitions(column_names, num_tablets)
    .set_range_partition_columns(column_names)
    .split_rows(splits)
    .Create();
  delete table_creator;
  return s;
}


Status InsertRows(const std::string& tableName,
		int cols,
		int num_rows,
		size_t chunk_size,
		int iteration)
{
  shared_ptr<KuduClient> client;
  CreateClient(master_host, 
	       &client);

  Grid::Timer timer("\tDONE::Insert Rows:" + Grid::to_string(iteration));


  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(client->OpenTable(tableName,
				  &table));
  
  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  KUDU_RETURN_NOT_OK(session->SetMutationBufferSpace(BUFFER_SIZE));
  session->SetTimeoutMillis(5000);

  int chunk_counter = 0;
  const int counter_flush_target = num_rows/chunk_size;
  const int slice = num_rows * iteration;

  KUDU_LOG(INFO)
    << "\tSTART::Inserting rows:" << slice << " to " << slice + num_rows
    << " flushing:" << counter_flush_target;
  
  for (int i = 0; i < num_rows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32("key", slice + i));
    //KUDU_CHECK_OK(row->SetString("group_1", Grid::GetString(2)));
    for(int j = 0; j < cols; ++j)
      {
	KUDU_CHECK_OK(row->SetInt32(1+j,slice+i+j));
      }
    KUDU_CHECK_OK(session->Apply(insert));

    if(++chunk_counter == counter_flush_target)
      {
	chunk_counter = 0;
	Grid::Timer t("\t\tFlush-" + Grid::to_string(i/chunk_size));
	KUDU_CHECK_OK(session->Flush());
      }
  }

  Status s;

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

int main(int argc, char* argv[]) {
  KUDU_LOG(INFO) << "Running with Kudu client version: " <<
      kudu::client::GetShortVersionString();
  KUDU_LOG(INFO) << "Long version info: " <<
      kudu::client::GetAllVersionInfo();

  if (argc < 5) {
    KUDU_LOG(FATAL) << "usage: " << argv[0] << " <rows> <cols> <number of chunks> <concurrency=1>";
  }
  const int rows = atoi(argv[1]);
  const int cols = atoi(argv[2]);
  const int number_of_chunks = atoi(argv[3]);
  const int concurrency = argc == 5 ? atoi(argv[4]) : std::thread::hardware_concurrency();

  const string kTableName = "t_"  + Grid::to_string(rows) + "_" + Grid::to_string(2 + cols);
  // Enable verbose debugging for the client library.
  // kudu::client::SetVerboseLogLevel(2);

  shared_ptr<KuduClient> client;

  // Create and connect a client.
  KUDU_CHECK_OK(CreateClient(master_host, &client));
  KUDU_LOG(INFO) << "Created a client connection";

  // Disable the verbose logging.
  kudu::client::SetVerboseLogLevel(0);

  // Create a schema.
  KuduSchema schema(CreateSchema(cols));
  KUDU_LOG(INFO) << "Created a schema";

  // Create a table with that schema.
  bool exists;
  KUDU_CHECK_OK(DoesTableExist(client, kTableName, &exists));
  if (exists) {
    client->DeleteTable(kTableName);
    client->DeleteTable(kTableName + "_threaded");
    KUDU_LOG(INFO) << "Deleting old table before creating new one";
  }
  KUDU_CHECK_OK(CreateTable(client, kTableName, schema, NUMBER_OF_TABLETS, rows));
  KUDU_CHECK_OK(CreateTable(client, kTableName + "_threaded", schema, NUMBER_OF_TABLETS, rows));
  KUDU_LOG(INFO) << "Created a tables:" << kTableName << "_threaded and " << kTableName;

  {
    Grid::Timer t("Non-Threaded insert");
    InsertRows(kTableName, cols, rows, number_of_chunks, 0);
  }

  {
    Grid::Timer t("Threaded insert:" + Grid::to_string(concurrency));

    vector<std::thread> threads;
    for(int i = 0; i < concurrency; ++i)
      {
	threads.push_back(std::thread(InsertRows,
				      kTableName + "_threaded",
				      cols,
				      rows/concurrency,
				      number_of_chunks/concurrency,
				      i));
      }
    for(vector<std::thread>::iterator iter(threads.begin()); iter != threads.end(); ++iter)
      {
	iter->join();
      }
  }
  // Done!
  KUDU_LOG(INFO) << "Done";

  return 0;
}
