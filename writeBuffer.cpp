// Written by Billy Lai
// 6/25/18
// Updating a LINEITEM field within a FlatBuffer of FlexBuffers

#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <sys/time.h>
#include "flatbuffers/flexbuffers.h"
#include "flatflexV2_generated.h"

using namespace std;
using namespace Tables;

enum DataType {TypeInt = 1, TypeDouble, TypeChar, TypeDate, TypeString};
typedef flatbuffers::FlatBufferBuilder FBB;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<rows> rows_vector;

struct bucket_t {
	FBB fb;
//	delete_vector dead;
	rows_vector vec;
};

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);
void assertionCheck(int, int, int, int, float, float, float, float, int8_t, int8_t, vector<int>, vector<int>, vector<int>, string, string, string,
			int, int, int, int, float, float, float, float, int8_t, int8_t,
				flexbuffers::Vector, flexbuffers::Vector, flexbuffers::Vector, flexbuffers::String, flexbuffers::String, flexbuffers::String);

void promptFileName(ifstream&, string&);
void promptVariables(uint32_t&, uint32_t&, uint32_t&);
vector<int> getSchema(vector<string>&, string&);
vector<string> getNextRow(ifstream& inFile);
int findKeyIndexWithinSchema(string, string);
uint32_t getOId(uint64_t, int);

int main()
{
	map<uint32_t, flatbuffers::FlatBufferBuilder> FBmap;
	map<uint32_t, flatbuffers::FlatBufferBuilder>::iterator it;
	FBB *fbb;
//	delete_vector *dead;
	rows_vector *rows;
// ----------------------------------------------Open .csv File---------------------------------------------------
	ifstream inFile;
	string fileName = "";
	promptFileName(inFile, fileName);
// ---------------------------------------------Prompt for Configurable Variables------------------------------------
	string input = "";
	uint32_t num_objs = 0;
	uint32_t num_buckets = 0;
	uint32_t rows_flush = 0;
	uint32_t rows_total = 0;

	promptVariables(num_objs, rows_flush, rows_total);
	num_buckets = num_objs;
// -------------------------------------Read Schema File---------------------------------------
	string schemaFileName = "";
	vector<string> compositeKey;
	vector<int> schema = getSchema(compositeKey, schemaFileName); // returns vector of enum types
// ------------------------------------Get Rows and Load into FB-------------------------------
	vector<string> parsedRow = getNextRow(inFile);
	int rowId = 500;
	// Create Flexbuffer from Parsed Row and Schema
	flexbuffers::Builder flx0;
	flx0.Vector([&]() {
		for(int i=0;i<(int)schema.size();i++) {
			switch(schema[i]) {
				case TypeInt:
					flx0.Int( atoi(parsedRow[i].c_str()) );
					break;
				case TypeDouble:
					flx0.Double( stod(parsedRow[i]) );
					break;
				case TypeChar:
					flx0.Int( parsedRow[i][0] );
					break;
				case TypeDate:
					flx0.String( parsedRow[i]);
					break;
				case TypeString:
					flx0.String( parsedRow[i]);
					break;
				default:
					flx0.String("EMPTY");
					break;
			}
		}
	});
	flx0.Finish();
	// Get pointer to FlexBuffer
	vector<uint8_t> flxPtr = flx0.GetBuffer();

	// Hash Composite Key
	vector<int> keyIndexes;
	for(int i=0; i<(int)compositeKey.size();i++) {
		keyIndexes.push_back( findKeyIndexWithinSchema(schemaFileName, compositeKey[i]) );
		cout<<keyIndexes[i]<<endl;
	}

	uint64_t hashKey=0, upper=0, lower=0;
	upper = parsedRow[keyIndexes[0]];
	hashKey = upper << 32;
	if( keyIndexes.size() > 1)
		lower = parsedRow[keyIndexes[1]];
	hashKey = hashKey | lower;

	uint32_t oid = getOid(hashKey, num_objs);

	// Get FB and insert
	bucket_t current_bucket;

	it = FBmap.find(oid);
	if(it != FBmap.end()) {
		current_bucket = &(it->second);
	}
	else {
		fbb = new FBB();
//		dead = new delete_vector();
		rows= new rows_vector();
//		bucket_t bucket(*fbb, *dead, *rows);
		bucket_t bucket(*fbb, *rows);
		pair<uint32_t, bucket_t > data(oid, bucket);
		it = FBmap.insert(data);
		current_bucket = &(FBmap.find(oid)->second);
	}
	fbb = &(current_bucket.fb);
//	dead = &(current_bucket.dead);
	rows = &(current_bucket.vec);
	
	auto flxSerial = fbb->CreateVector(flxPtr);
	auto row0 = CreateRows(*fbb, rowID++, flxSerial);
	rows->push_back(row0);
//	dead.push_back('0');

	// Flush if rows_flush was met
	if( (int)rows->size() >= rows_flush) {
		auto rows_vec = fbb->CreateVector(*rows);
		int version = 1;
		auto tableOffset = CreateTable(*fbb, version, rows_vec);
		fbb->Finish(tableOffset);

		//Flush to Ceph

		fbb->Reset();
	}


// -----------------------------------------------------Initialize FlatBuffer ----------------------------------------
	//	 Create a 'FlatBufferBuilder', which will be used to create our LINEITEM FlexBuffers
//	flatbuffers::FlatBufferBuilder fbbuilder(1024);
//	vector<flatbuffers::Offset<Rows>> rows_vector;

i	// Get Pointer to FlexBuffer Row
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

void promptFileName(ifstream& inFile, string& fileName) {
	cout<<"Which file will we load from? : ";
	getline(cin, fileName);
	inFile.open(fileName, std::ifstream::in);
	while(!inFile || strcmp(fileName.c_str(),"q") == 0 || strcmp(fileName.c_str(),"Q") == 0 )
	{
		if(strcmp(fileName.c_str(),"q")==0 || strcmp(fileName.c_str(),"Q")==0 )
			exit(0);

		cout<<"Cannot open file! \t\t(Press Q or q to quit program)"<<endl;
		cout<<"Which file will we load from? : ";
		getline(cin, fileName);
		inFile.open(fileName, ifstream::in);
	}
	cout<<"'"<<fileName<<"' was sucessfully opened!"<<endl<<endl;
}
void promptVariables(uint32_t& num_objs, uint32_t& rows_flush, uint32_t& rows_total) {
	string input = "";
	
	while(true) {
		cout<<"How many objects?: ";
		getline(cin, input);
		stringstream myStream(input);
		if(myStream >> num_objs)
			break;
		cout<<"Invalid number, please try again"<<endl;
	}
	while(true) {
		cout<<"How many rows until buckets flushed?: ";
		getline(cin, input);
		stringstream myStream(input);
		if(myStream >> rows_flush)
			break;
		cout<<"Invalid number, please try again"<<endl;
	}
	while(true) {
		cout<<"How many rows would you like to read?: ";
		getline(cin, input);
		stringstream myStream(input);
		if(myStream >> rows_total)
			break;
		cout<<"Invalid number, please try again"<<endl;
	}
	cout<<endl;
	cout<<"Number of objects: "<<num_objs<<endl;	
	cout<<"Number of rows until buckets are flushed: "<<rows_flush<<endl;
	cout<<"Number of rows to be read: "<<rows_total<<endl<<endl;
}
vector<int> getSchema(vector<string>& compositeKey, string& schemaFileName) {
	ifstream schemaFile;
	string fileName = "";
	vector<int> schema;

	// Get Schema File
	cout<<"Which file will we get the schema from? : ";
	getline(cin, fileName);
	schemaFile.open(fileName, std::ifstream::in);
	while(!schemaFile || strcmp(fileName.c_str(),"q")==0 || strcmp(fileName.c_str(),"Q")==0 )
	{
		if(strcmp(fileName.c_str(),"q")==0 || strcmp(fileName.c_str(),"Q")==0 )
			exit(0);

		cout<<"Cannot open file! \t\t(Press Q or q to quit program)"<<endl;
		cout<<"Which file will we get the schema from? : ";
		getline(cin, fileName);
		schemaFile.open(fileName, ifstream::in);
	}
	// Parse Schema File for Data Types
	string pKeys = "";
	getline(schemaFile, pKeys);
	compositeKey = split(pKeys,' ');

	string line;
	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		schema.push_back( atoi(parsedData[3].c_str()) );
	}

	cout<<"'"<<fileName<<"' was sucessfully read and schema vector passed back!"<<endl<<endl;
	schemaFileName = fileName;
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
int findKeyIndexWithinSchema(string schemaFileName, string key) {
	ifstream schemaFile;
	schemaFile.open(schemaFileName, ifstream::in);
	string line;
	getline(schemaFile, line); // skip composite keys header

	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		if( strcmp(parsedData[1].c_str(), key.c_str()) == 0) {
			schemaFile.close();
			return atoi(parsedData[0].c_str());
		}
	}
	// Else key was not found in schema
	schemaFile.close();
	return -1;
}
uint32_t getOid(uint64_t hashKey, uint32_t num_objs) {
	return (uint32_t) (hashKey % (uint64_t)num_objs);
}
