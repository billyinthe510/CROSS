// Written by Billy Lai
// 6/25/18
// Writing rows from files to Ceph

#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <unistd.h>	// for getOpt
#include "flatbuffers/flexbuffers.h"
#include "flatflexV2_generated.h"

using namespace std;
using namespace Tables;

enum DataType {TypeInt = 1, TypeDouble, TypeChar, TypeDate, TypeString};
typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
//typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Rows>> rows_vector;

typedef struct {
	int oid;
	fbBuilder *fb;
//	delete_vector dead;
	rows_vector *rowsv;
} bucket_t;

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);
void assertionCheck(int, int, int, int, float, float, float, float, int8_t, int8_t, vector<int>, vector<int>, vector<int>, string, string, string,
			int, int, int, int, float, float, float, float, int8_t, int8_t,
				flexbuffers::Vector, flexbuffers::Vector, flexbuffers::Vector, flexbuffers::String, flexbuffers::String, flexbuffers::String);

void promptDataFile(ifstream&, string&);
vector<int> getSchema(vector<string>&, string&);
uint32_t promptIntVariable(string, string);

void promptVariables(uint32_t&, uint32_t&, uint32_t&);
vector<string> getNextRow(ifstream& inFile);
void getFlxBuffer(flexbuffers::Builder *, vector<string>, vector<int>);
int findKeyIndexWithinSchema(string, string);
uint64_t hashCompositeKey(string, vector<string>, vector<string>);
int32_t jumpConsistentHash(uint64_t, int32_t);

int main(int argc, char *argv[])
{
	ifstream inFile;
	string file_name = "";
	string schema_file_name = "";
	vector<string> composite_key;
	vector<int> schema;
	uint32_t num_objs = 0;
	uint32_t num_buckets = 0;
	uint32_t flush_rows = 0;
	uint32_t read_rows = 0;

	int opt;
	while( (opt = getopt(argc, argv, "f:s:o:r:n:")) != -1) {
		switch(opt) {
			case 'f':
				// Open .csv file
				file_name = optarg;
				promptDataFile(inFile, file_name);
				break;
			case 's':
				// Get Schema (vector of enum types), schema file name, composite key
				schema_file_name = optarg;
				schema = getSchema(composite_key, schema_file_name);	// returns vector of enum data types and composite keys
				break;
			case 'o':
				// Set # of Objects
				num_objs = promptIntVariable("objects", optarg);
				break;
			case 'r':
				// Set # of Rows to Read Until Flushed
				flush_rows = promptIntVariable("rows until flush", optarg);
				break;
			case 'n':
				// Set # of Total Rows to Read
				read_rows = promptIntVariable("rows to read", optarg);
				break;
			
			
			
		}
	}

	map<uint32_t, bucket_t> FBmap;
	map<uint32_t, bucket_t>::iterator it;
// ---------------------------------------------Prompt for Configurable Variables------------------------------------
	string input = "";

//	promptVariables(num_objs, rows_flush, rows_total);
		num_objs = flush_rows = read_rows = 40;
	num_buckets = num_objs;
// -------------------------------------------Get Rows and Load into FB--------------------------------
	vector<string> parsedRow = getNextRow(inFile);
	flexbuffers::Builder *flx = new flexbuffers::Builder();
	getFlxBuffer(flx, parsedRow, schema);
	// Get pointer to FlexBuffer
	vector<uint8_t> flxPtr = flx->GetBuffer();
	cout<<"FlexBuff size: "<<flxPtr.size()<<endl;
// ---------------------------------------------Hash Composite Key-------------------------------------------------
	uint64_t hashKey = hashCompositeKey(schema_file_name, composite_key, parsedRow);
// --------------------------------------------Get Oid Using HashKey-----------------------------------------------
	int32_t oid = jumpConsistentHash(hashKey, num_objs);
	cout<<"OID :  "<<oid<<endl;

// -----------------------------------------Get FB and insert-----------------------------------------------
	bucket_t current_bucket;
	fbb fbPtr;
//	delete_vector *dead;
	rows_vector *rowsPtr;


	// Get FB from map or use new FB
	it = FBmap.find(oid);
	if(it != FBmap.end()) {
		current_bucket = it->second;
	}
	else
	{
		current_bucket.oid = oid;
		current_bucket.fb = new fbBuilder();
		current_bucket.rowsv = new rows_vector();
		FBmap.insert(pair<uint32_t, bucket_t>(oid,current_bucket));
	}

	fbPtr = current_bucket.fb;
//	dead = &(current_bucket.dead);
	rowsPtr = current_bucket.rowsv;

	auto flxSerial = fbPtr->CreateVector(flxPtr);
	int rowID = 500;
	auto rowOffset = CreateRows(*fbPtr, rowID++, flxSerial);
	rowsPtr->push_back(rowOffset);

//	dead.push_back('0');

	
	// Flush if rows_flush was met
	if( (int)rowsPtr->size() >= 1) {
		auto rows_vecOffset = fbPtr->CreateVector(*rowsPtr);
		int version = 1;
		auto tableOffset = CreateTable(*fbPtr, version, rows_vecOffset);
		fbPtr->Finish(tableOffset);

		// Flush to Ceph Here TO OID bucket with n Rows
		// Set Delete Vector
		printf("Flushing %d to Ceph\n", oid);

		printf("Clearing FB Ptr and RowsVector Ptr, Delete Bucket from Map\n");
		FBmap.erase(oid);
		fbPtr->Reset();
		delete fbPtr;
		rowsPtr->clear();
		delete rowsPtr;
		delete flx;
	}

	// Iterate over map and flush each bucket


// -----------------------------------------------------Initialize FlatBuffer ----------------------------------------
	//	 Create a 'FlatBufferBuilder', which will be used to create our LINEITEM FlexBuffers
//	flatbuffers::FlatBufferBuilder fbbuilder(1024);
//	vector<flatbuffers::Offset<Rows>> rows_vector;

	// Get Pointer to FlexBuffer Row
//	vector<uint8_t> flxPtr = flx0.GetBuffer();
//	int flxSize = flx0.GetSize();
//	cout<<"FlexBuffer Size: "<<flxSize<<" bytes"<<endl;

/*	auto vec = flexbuffers::GetRoot(flxPtr).AsVector();
	int v0 = vec[0].AsInt32();
	int v1 = vec[1].AsInt32();
	int v2 = vec[2].AsInt32();
	int v3 = vec[3].AsInt32();
	double v4 = vec[4].AsDouble();
	double v5 = vec[5].AsDouble();
	double v6 = vec[6].AsDouble();
	double v7 = vec[7].AsDouble();
	char v8 = char(vec[8].AsInt32());
	char v9 = (char)vec[9].AsInt32();
	string v10 = vec[10].AsString().str();
	string v11 = vec[11].AsString().str();
	string v12 = vec[12].AsString().str();
	string v13 = vec[13].AsString().str();
	string v14 = vec[14].AsString().str();
	string v15 = vec[15].AsString().str();
	cout<<v0<<endl;
	cout<<v1<<endl;
	cout<<v2<<endl;
	cout<<v3<<endl;
	cout<<v4<<endl;
	printf("%.8f\n",v5);
	cout<<v6<<endl;
	cout<<v7<<endl;
	cout<<v8<<endl;
	cout<<v9<<endl;
	cout<<v10<<endl;
	cout<<v11<<endl;
	cout<<v12<<endl;
	cout<<v13<<endl;
	cout<<v14<<endl;
	cout<<v15<<endl;
*/

	// Hash on Composite Key
		
/*
// ---------------------------------------Create 4MB of FlexBuffers----------------------------------------
	int nRows = 20900;
	for(int i=0;i<nRows;i++) {
		// Serialize buffer into a Flatbuffer::Vector
		auto flxSerial = fbbuilder.CreateVector(flxPtr);
		// Create a Row from FlexBuffer and new ID
		auto row0 = CreateRows(fbbuilder, rowID++, flxSerial);
		// Push new row onto vector of rows
		rows_vector.push_back(row0);
	}
//----------------------------------------Create FlatBuffer of FlexBuffers --------------------------------
	// Serializing vector of Rows adds 12 bytes
	auto rows_vec = fbbuilder.CreateVector(rows_vector);
	int version = 1;
	auto tableOffset = CreateTable(fbbuilder, version, rows_vec);
	fbbuilder.Finish(tableOffset);

// --------------------------------------------Start Timing Rows-------------------------------------------
//
	// Get Pointer to FlatBuffer
	uint8_t *buf = fbbuilder.GetBufferPointer();
	int size = fbbuilder.GetSize();
	cout<<"Buffer Size (FlatBuffer of FlexBuffers): "<<size<<" bytes"<<endl;
	// Check number of FlexBuffers in FlatBuffer
	volatile auto table = GetMutableTable(buf);
	int recsCount = table->data()->size();
	cout<<"Row Count: "<<recsCount<<endl;

				table = GetMutableTable(buf);
				const flatbuffers::Vector<flatbuffers::Offset<Rows>>* recs = table->data();
				for(int k=0;k<rowNum;k++) {
					auto flxRoot = recs->Get(k)->rows_flexbuffer_root();
					auto mutatedCheck = flxRoot.AsVector()[1].MutateUInt(556);
*/
	return 0;
}

std::vector<std::string> split(const std::string &s, char delim) {
	std::istringstream ss(s);
	std::string item;
	std::vector<std::string> tokens;
	while( getline(ss, item, delim)) {
		tokens.push_back(item);
	}
	return tokens;
}
std::vector<int> splitDate(const std::string &s) {
	std::istringstream ss(s);
	std::string item;
	std::vector<int> tokens;
	while( getline(ss, item, '-')) {
		tokens.push_back(atoi(item.c_str()));
	}
	return tokens;
}
void assertionCheck(int orderkey, int partkey, int suppkey, int linenumber, float quantity, float extendedprice, float discount, float tax,
			int8_t returnflag, int8_t linestatus, vector<int> shipdate, vector<int> receiptdate, vector<int> commitdate,
			string shipinstruct, string shipmode, string comment,
			int _orderkey, int _partkey, int _suppkey, int _linenumber, float _quantity, float _extendedprice, float _discount, float _tax,
			int8_t _returnflag, int8_t _linestatus, flexbuffers::Vector _shipdate, flexbuffers::Vector _receiptdate, flexbuffers::Vector _commitdate,
			flexbuffers::String _shipinstruct, flexbuffers::String _shipmode, flexbuffers::String _comment) {

	assert(_orderkey == orderkey);
	assert(_partkey == partkey);
	assert(_suppkey == suppkey);
	assert(_linenumber == linenumber);
	
	assert(_quantity == quantity);
	assert(_extendedprice == extendedprice);
	assert(_discount == discount);
	assert(_tax == tax);

	assert(_returnflag == returnflag);
	assert(_linestatus == linestatus);

	assert(_shipdate[0].AsInt32() == shipdate[0]);
	assert(_shipdate[1].AsInt32() == shipdate[1]);
	assert(_shipdate[2].AsInt32() == shipdate[2]);
	assert(_receiptdate[0].AsInt32() == receiptdate[0]);
	assert(_receiptdate[1].AsInt32() == receiptdate[1]);
	assert(_receiptdate[2].AsInt32() == receiptdate[2]);
	assert(_commitdate[0].AsInt32() == commitdate[0]);
	assert(_commitdate[1].AsInt32() == commitdate[1]);
	assert(_commitdate[2].AsInt32() == commitdate[2]);

	assert( strcmp( _shipinstruct.c_str(), shipinstruct.c_str() ) == 0);
	assert( strcmp( _shipmode.c_str(), shipmode.c_str() ) == 0);
	assert( strcmp( _comment.c_str(), comment.c_str() ) == 0);

}

void promptDataFile(ifstream& inFile, string& file_name) {
	// Open File
	inFile.open(file_name, std::ifstream::in);

	// If file didn't open, prompt for data file
	while(!inFile)
	{
		cout<<"Cannot open file!"<<endl;
		cout<<"Which file will we load from? : ";
		getline(cin, file_name);
		inFile.open(file_name, ifstream::in);
	}
	cout<<"'"<<file_name<<"' was sucessfully opened!"<<endl<<endl;
}
uint32_t promptIntVariable(string variable, string num_string) {
	stringstream myStream(num_string);
	uint32_t n = 0;

	while( !(myStream >> n) ) {
		cout<<"How many "<<variable<<"?: ";
		getline(cin, num_string);
		stringstream newStream(num_string);
		if(newStream >> n)
			break;
		cout<<"Invalid number, please try again"<<endl;
	}
	cout<<"Number of "<<variable<<": "<<n<<endl<<endl;
	return n;
}

vector<int> getSchema(vector<string>& compositeKey, string& schema_file_name) {
	ifstream schemaFile;
	vector<int> schema;

	// Try to open schema file
	schemaFile.open(schema_file_name, std::ifstream::in);

	// If schema file didn't open, prompt for schema file name
	while(!schemaFile)
	{
		if(strcmp(schema_file_name.c_str(),"q")==0 || strcmp(schema_file_name.c_str(),"Q")==0 )
			exit(0);

		cout<<"Cannot open file! \t\t(Press Q or q to quit program)"<<endl;
		cout<<"Which file will we get the schema from? : ";
		getline(cin, schema_file_name);
		schemaFile.open(schema_file_name, ifstream::in);
	}
	// Parse Schema File for Composite Key
	string line = "";
	getline(schemaFile, line);
	compositeKey = split(line,' ');

	// Parse Schema File for Data Types
	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		schema.push_back( atoi(parsedData[3].c_str()) );
	}

	cout<<"'"<<schema_file_name<<"' was sucessfully read and schema vector passed back!"<<endl<<endl;
	schemaFile.close();
	
	return schema;
}
vector<string> getNextRow(ifstream& inFile) {
	// Get a row
	string row;
	getline(inFile, row);
	// Split by '|' deliminator
	return split(row, '|');
}
void getFlxBuffer(flxBuilder *flx, vector<string> parsedRow, vector<int> schema) {
	// Create Flexbuffer from Parsed Row and Schema
	flx->Vector([&]() {
		for(int i=0;i<(int)schema.size();i++) {
			switch(schema[i]) {
				case TypeInt:
					flx->Int( atoi(parsedRow[i].c_str()) );
					break;
				case TypeDouble:
					flx->Double( stod(parsedRow[i]) );
					break;
				case TypeChar:
					flx->Int( parsedRow[i][0] );
					break;
				case TypeDate:
					flx->String( parsedRow[i]);
					break;
				case TypeString:
					flx->String( parsedRow[i]);
					break;
				default:
					flx->String("EMPTY");
					break;
			}
		}
	});
	flx->Finish();
}
int findKeyIndexWithinSchema(string schemaFileName, string key) {
	ifstream schemaFile;
	schemaFile.open(schemaFileName, ifstream::in);
	string line;
	getline(schemaFile, line); // skip composite keys header

	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		// Hardcoded to know schema file 
		//	has 'name' field as 2nd column of schema file
		//	and has 'field number' as 1st column of schema file
		//	field number | name | data type | data type enum
		if( strcmp(parsedData[1].c_str(), key.c_str()) == 0) {
			schemaFile.close();
			return atoi(parsedData[0].c_str());
		}
	}
	// Else key was not found in schema
	schemaFile.close();
	return -1;
}
uint64_t hashCompositeKey(string schemaFileName, vector<string> compositeKey, vector<string> parsedRow) {
	// Get the Index of Key/s from SchemaFile
	vector<int> keyIndexes;
	for(int i=0; i<(int)compositeKey.size();i++) {
		keyIndexes.push_back( findKeyIndexWithinSchema(schemaFileName, compositeKey[i]) );
	}
	// Hash the Composite Key
	uint64_t hashKey=0, upper=0, lower=0;
	stringstream(parsedRow[keyIndexes[0]]) >> upper;
	hashKey = upper << 32;
	if( keyIndexes.size() > 1) {
		stringstream(parsedRow[keyIndexes[1]]) >> lower;
		hashKey = hashKey | lower;
	}
	return hashKey;
}
int32_t jumpConsistentHash(uint64_t key, int32_t num_buckets) {
	int64_t b=-1, j=0;
	while(j < num_buckets) {
		b = j;
		key = key * 286293355588894185ULL + 1;
		j = (b+1) * (double(1LL << 31) / double((key>>33) + 1));
	}
	return b;
}
