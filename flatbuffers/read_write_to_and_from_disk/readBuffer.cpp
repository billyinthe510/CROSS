/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// This is still in progress
// Currently assumes one flatbuffer per object file

#include <fcntl.h> // system call open
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <unistd.h>	// for getOpt
#include "../header_files/flatbuffers/flexbuffers.h"
#include "../header_files/skyhookv1_generated.h"

using namespace std;
using namespace Tables;

const int offset_to_skyhook_version = 4;
const int offset_to_schema_version = 6;
const int offset_to_table_name = 8;
const int offset_to_schema = 10;
const int offset_to_delete_vec = 12;
const int offset_to_rows_vec = 14;
const int offset_to_nrows = 16;

const int offset_to_RID = 4;
const int offset_to_nullbits_vec = 6;
const int offset_to_data = 8;

enum DataType {TypeInt = 1, TypeDouble, TypeChar, TypeDate, TypeString};

typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Row>> rows_vector;

typedef struct {
	uint64_t oid;
	uint32_t nrows;
	string table_name;
	fbb fb;
	delete_vector *deletev;
	rows_vector *rowsv;
} bucket_t;

typedef struct {
	int skyhook_version;
	int schema_version;
	string table_name;
	string schema;
	delete_vector delete_vec;
	int rows_offset;
	int nrows;
} root_header;

typedef struct {
	int64_t RID;
	vector<uint64_t> nullbits;
	int data_offset;	
} row_header;

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);
string int_to_binary(uint64_t);
void initializeNullbits(vector<uint64_t> *);
void helpMenu();
void promptDataFile(string&, int&);
vector<int> getSchema(vector<string>&, string&);
uint32_t promptIntVariable(string, string);
bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &, uint64_t);
void deleteBucket(bucket_t *, map<uint64_t, bucket_t *> &, uint64_t, fbb, delete_vector *, rows_vector *);

vector<string> getNextRow(ifstream& inFile);
void getFlxBuffer(flexbuffers::Builder *, vector<string>, vector<int>, vector<uint64_t> *);
int findKeyIndexWithinSchema(string, string);
uint64_t hashCompositeKey(string, vector<string>, vector<string>);
uint64_t jumpConsistentHash(uint64_t, uint64_t);

int readOffset(int, int, uint8_t *);
int readRootHeader(int, root_header *);
int readRowOffsets(int, int, vector<int> &);
int readRowHeader(int, int, row_header *);

void find_vtable_given_row_ptr(int fd, int row_ptr);

int main(int argc, char *argv[])
{
	int fd;
	string file_name = "";
	string schema_file_name = "";
	vector<string> composite_key;
	vector<int> schema;
	uint64_t num_objs = 0;
// ----------------------------------------Verify Configurable Variables or Prompt For Them------------------------
	int opt;
	while( (opt = getopt(argc, argv, "hf:s:o:")) != -1) {
		switch(opt) {
			case 'f':
				// Open object file
				file_name = optarg;
				promptDataFile(file_name, fd);
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
			case 'h':
				helpMenu();
				exit(0);
				break;
		}
	}

	// Get object file size
	struct stat stat_buf;
	int rc = stat(file_name.c_str(), &stat_buf);
	int size = rc==0 ? stat_buf.st_size : -1;
	cout<<file_name<<"'s size: "<<size<<endl;	

	// Get ROOT_TABLE Header
	root_header *root = new root_header();
	rc =readRootHeader(fd, root);
	cout<<"skyhook version: "<<root->skyhook_version<<endl;
	cout<<"schema version: "<<root->schema_version<<endl;
	cout<<"table name: "<<root->table_name<<endl;
	cout<<"schema: "<<root->schema<<endl;
	cout<<"delete vector: [";
	for(int i=0;i< (int)root->delete_vec.size();i++) {
		cout<<(int)root->delete_vec[i];
		if(i != (int)root->delete_vec.size()-1 )
			cout<<", ";
	}
	cout<<"]"<<endl;
	cout<<"row offset: "<<root->rows_offset<<endl;
	cout<<"nrows: "<<root->nrows<<endl;
	cout<<endl;
	
	// Get ROW_TABLE Offsets
	vector<int> row_offsets;
	readRowOffsets(fd, root->rows_offset, row_offsets);
	
	// Get ROW_TABLE Headers
	vector<row_header *>rows;
	for(int i=0;i<root->nrows;i++) {
		row_header *row = new row_header();
		readRowHeader(fd, row_offsets[i], row);
		rows.push_back(row);
	}

	// Get Data FlexBuffer for each row
	for(int i=0;i< (int)rows.size();i++) {
		cout<<i<<"'s RID: "<<rows[i]->RID<<endl;
	}

	int numRows = 4;

/*
	vector<uint32_t> row_offsets;
	// For each Row, get the offset to Row FB (Offsets to Row FB are 32 bit values)
	for(int i=0;i<numRows;i++) {
		current_offset += 4;

		// Get ROW_TABLE Offset
		lseek(fd, current_offset, SEEK_SET);
		if(read(fd,reinterpret_cast<char *>(&offset), 4) != 4)
			return -1;
		int row_root_ptr = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
		int row_ptr = current_offset + row_root_ptr;

		cout<<"row is at "<<row_ptr<<endl;
		row_offsets.push_back(row_ptr);

		// Get ROW_TABLE's vTable Offset
		lseek(fd,row_ptr,SEEK_SET);	
		if(read(fd,reinterpret_cast<char *>(&offset), 4) != 4)
			return -1;
		int row_vtable_ptr = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
		int row_vtable_offset = row_ptr - row_vtable_ptr;
		
		lseek(fd,row_vtable_offset,SEEK_SET);
		if(read(fd,reinterpret_cast<char *>(&offset), 4) != 4)
			return -1;
		int data_ptr = (int)offset[0];

		cout<<endl;
	}
*/
		
	const int file_size = size;
	lseek(fd,0,SEEK_SET);
	uint8_t bytes[file_size];

	if(read(fd,reinterpret_cast<char *>(&bytes),file_size) !=file_size)
		return -1;

	for(int i=0;i<file_size;i++) {
//		if(bytes[i] == 97 || bytes[i] == 98 || bytes[i] == 111 || bytes[i] == 118)
//			cout<<"\t\t\t";
//		int diff = i+bytes[i];
//		if(diff ==  784|| diff == 464 || diff == 156)
//			cout<<"\t\t\t\t";
		//printf("Byte %d: %u\n",i,bytes[i]);
	}


	lseek(fd,0,SEEK_SET);
	uint8_t contents[file_size];
	if(read(fd,reinterpret_cast<char *>(&contents), file_size) != file_size)
		return -1;
	auto table = GetTable(contents);
	const flatbuffers::Vector<flatbuffers::Offset<Row>>* recs = table->rows();
	for(int i=0;i<numRows;i++) {
		auto RID = recs->Get(i)->RID();
		cout<<"RID: "<<RID<<endl;
		auto flxRoot = recs->Get(i)->data_flexbuffer_root();
		auto comment = flxRoot.AsVector()[15].AsString();
		cout<<"one of them: "<<comment.str()<<endl;
	}

	delete root;
	for(int i=0;i<root->nrows;i++)
		delete rows[i];

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

void helpMenu() {
	printf("-h HELP MENU\n");
	printf("\t-f [file_name]\n");
	printf("\t-s [schema_file_name]\n");
	printf("\t-o [number_of_objects]\n");
	printf("\t-r [number_of_rows_until_flush]\n");
	printf("\t-n [number_of_rows_to_read]\n");
}

void promptDataFile(string& file_name, int &fd) {
	ifstream inFile;
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
	inFile.close();
	fd = open(file_name.c_str(), O_RDONLY);
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

void getFlxBuffer(flxBuilder *flx, vector<string> parsedRow, vector<int> schema, vector<uint64_t> *nullbits) {
	bool nullFlag = false;
	string nullcmp = "NULL";
	// Create Flexbuffer from Parsed Row and Schema
	flx->Vector([&]() {
		for(int i=0;i<(int)schema.size();i++) {
			nullFlag = false;
			if(strcmp(parsedRow[i].c_str(), nullcmp.c_str() ) == 0)
				nullFlag = true;

			if(nullFlag) {
				// Mark nullbit
				uint64_t nullMask = 0x00;
				if(i<64) {
					nullMask = 1lu << (63-i);
					nullbits[0][0] |=  nullMask;
				}
				else {
					nullMask = 1lu << (63- (i-64));
					nullbits[0][1] |= nullMask;
				}
				// Put a dummy variable to hold the index for future updates
				switch(schema[i]) {
					case TypeInt:
						flx->Int(0);
						break;
					case TypeDouble:
						flx->Double(0.0);
						break;
					case TypeChar:
						flx->Int('0');
						break;
					case TypeDate:
						flx->String("1993-11-10");
						break;
					case TypeString:
						flx->String("This will be pooled with strings");
						break;
					default:
						flx->String("EMPTY");
						break;
				}
			}
			else {
				switch(schema[i]) {
					case TypeInt: {
						flx->Int( atoi(parsedRow[i].c_str()) );
						break;
					}
					case TypeDouble: {
						flx->Double( stod(parsedRow[i]) );
						break;
					}
					case TypeChar: {
						flx->Int( parsedRow[i][0] );
						break;
					}
					case TypeDate: {
						flx->String( parsedRow[i]);
						break;
					}
					case TypeString: {
						flx->String( parsedRow[i]);
						break;
					}
					default: {
						flx->String("EMPTY");
						break;
					}
				}
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

uint64_t jumpConsistentHash(uint64_t key, uint64_t num_buckets) {
	// Source:
	// A Fast, Minimal Memory, Consistent Hash Algorithm
	// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
	//
	int64_t b=-1l, j=0l;
	while(j < (int64_t)num_buckets) {
		b = j;
		key = key * 286293355588894185ULL + 1;
		j = (b+1) * (double(1LL << 31) / double((key>>33) + 1));
	}
	return b;
}

string int_to_binary(uint64_t value) {
	string output = "";
	for(int i=63;i>=0;i--)
	{
		if( value & ( 1lu<<i) )
			output += "1";
		else
			output += "0";
	}
	return output;
}

void initializeNullbits(vector<uint64_t> *nullbits) {
	nullbits->push_back((uint64_t) 0);
	nullbits->push_back((uint64_t) 0);
}

bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &FBmap, uint64_t oid) {
	bucket_t *bucketPtr;
	// Get FB from map or use new FB
	map<uint64_t, bucket_t *>::iterator it;
	it = FBmap.find(oid);
	if(it != FBmap.end()) {
		bucketPtr = it->second;
	}
	else
	{
		bucketPtr = new bucket_t();
		bucketPtr->oid = oid;
		bucketPtr->nrows = 0;
		bucketPtr->table_name = "LINEITEM";
		bucketPtr->fb = new fbBuilder();
		bucketPtr->deletev = new delete_vector();
		bucketPtr->rowsv = new rows_vector();
		FBmap.insert(pair<uint64_t, bucket_t *>(oid,bucketPtr));
	}
	return bucketPtr;
}

void deleteBucket(bucket_t *bucketPtr, map<uint64_t, bucket_t *> &FBmap, uint64_t oid, fbb fbPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {
		printf("Clearing FB Ptr and RowsVector Ptr, Delete Bucket from Map\n\n");
		
		FBmap.erase(oid);
		fbPtr->Reset();
		delete fbPtr;
		deletePtr->clear();
		delete deletePtr;
		rowsPtr->clear();
		delete rowsPtr;
		delete bucketPtr;
}

int readOffset(int fd, int current_offset, uint8_t *offset) {
	lseek(fd, current_offset , SEEK_SET);
	if(read(fd,reinterpret_cast<uint8_t *>(offset), sizeof(int)) != sizeof(int))
		return -1;
	return 0;
}


int readRootHeader(int fd, root_header *root) {
	uint8_t offset[4];
	int current_offset = 0;

	// Get root pointer of ROOT_TABLE ----------------------------------------------
	if( readOffset(fd, current_offset, offset) < 0 )
		return -1;
	int rootPtr = (int)offset[0];

	// Get vTable pointer for ROOT_TABLE -------------------------------------------
	if( readOffset(fd, rootPtr, offset) < 0)
		return -1;
	int vtablePtr = rootPtr - (int)offset[0];
	// Get skyhook_version ---------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_skyhook_version, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	root->skyhook_version = (int)offset[0];

	// Get schema_version ----------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_schema_version, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	root->schema_version = (int)offset[0];

	// Get table_name --------------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_table_name, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	current_offset += (int)offset[0];
	
		// Read size of table_name string
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	const int table_name_size = (int)offset[0];
		// Read table_name string
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	char table_name[table_name_size];
	if(read(fd, reinterpret_cast<char *>(&table_name), table_name_size) != table_name_size)
		return -1;
	string t(table_name,table_name_size);
	root->table_name = t;

	// Get schema ------------------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_schema, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	current_offset += (int)offset[0];
		// Read size of schema string
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	const int schema_size = (int)offset[0];
		// Read schema string
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	char schema[schema_size];
	if(read(fd, reinterpret_cast<char *>(&schema), schema_size) != schema_size)
		return -1;
	string s(schema, schema_size);
	root->schema = s;

	// Get delete_vector ------------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_delete_vec, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];

	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	current_offset += (int)offset[0];

		// Read size of delete_vector
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	const int delete_vec_size = (int)offset[0];
		// Read delete_vector
	current_offset += 4;
	lseek(fd, current_offset, SEEK_SET);	
	uint8_t delete_vec[delete_vec_size];
	if(read(fd, reinterpret_cast<char *>(&delete_vec), delete_vec_size) != delete_vec_size)
		return -1;
	for(int i=0;i<delete_vec_size;i++)
		root->delete_vec.push_back(delete_vec[i]);
	
	// Get rows offsets ---------------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_rows_vec, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	root->rows_offset = current_offset + (int)offset[0];

	// Get nrows -----------------------------------------------------------------------
	if( readOffset(fd, vtablePtr+offset_to_nrows, offset) < 0)
		return -1;
	current_offset = rootPtr + (int)offset[0];
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	root->nrows = (int)offset[0];

	return 0;
}
int readRowHeader(int fd, int row_table_ptr, row_header *row) {
	uint8_t offset[4];
	int current_offset = row_table_ptr;
	
	// Get ROW_TABLE's vTable Offset ----------------------------------------------------
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	int vtable_offset= (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
	int row_vtable_ptr = row_table_ptr - vtable_offset;
	
	// Get RID --------------------------------------------------------------------------
	if( readOffset(fd, row_vtable_ptr + offset_to_RID, offset) < 0)
		return -1;
	current_offset = row_table_ptr + (int)offset[0];
	lseek(fd, current_offset, SEEK_SET);
	if( read(fd, reinterpret_cast<uint8_t *>(&offset), sizeof(double)) != sizeof(double))
		return -1;
	row->RID = (int64_t)offset[0];
	
	// Get nullbits ---------------------------------------------------------------------
	if( readOffset(fd, row_vtable_ptr + offset_to_nullbits_vec, offset) < 0)
		return -1;
	current_offset = row_table_ptr + (int)offset[0];
		// Get offset to nullbits vector
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	current_offset += (int)offset[0];
		// Get size of nullbits vector
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	const int nullbits_size = (int)offset[0];
		// Copy nullbits
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	uint8_t nullbits[nullbits_size*sizeof(double)];
	if(read(fd, reinterpret_cast<char *>(&nullbits), nullbits_size*sizeof(double)) != (int)(nullbits_size*sizeof(double)))
		return -1;
	uint64_t null0 = (uint64_t)nullbits[0];
	uint64_t null1 = (uint64_t)nullbits[8];
	row->nullbits.push_back(null0);
	row->nullbits.push_back(null1);

	// Get data offset ------------------------------------------------------------------
		
	return 0;
}
int readRowOffsets(int fd, int rows_offset, vector<int> &row_offsets) {	
	uint8_t offset[4];
	int current_offset = rows_offset;

	// lseek to rows_vec starting point and get size of rows_vec
	if( readOffset(fd, current_offset, offset) < 0)
		return -1;
	int nrows = (int)offset[0];
	for(int i=0;i<nrows;i++) {
		current_offset += 4;
		// Get ROW_TABLE Offset

		if( readOffset(fd, current_offset, offset) < 0)
			return -1;
		int row_root_ptr = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
		int row_ptr = current_offset + row_root_ptr;
		row_offsets.push_back(row_ptr);
	}
	return 0;
}

void find_vtable_given_row_ptr(int fd, int row_ptr) {
	uint8_t offset[4];
	lseek(fd, row_ptr, SEEK_SET);
	if( read(fd, reinterpret_cast<char *>(&offset), 4) != 4)
		exit(0);
	
	int p = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
	int row_v = row_ptr - p;
	cout<<"row's vtable is at: "<<row_v<<endl;
}

