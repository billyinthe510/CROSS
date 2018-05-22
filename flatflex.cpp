// Written by Billy Lai
// 5/8/18
// Reading and Writing LineItem FlatBuffers

#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include "flatbuffers/flexbuffers.h"
#include "flatflex_generated.h"

using namespace std;
using namespace Tables;

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);

int main()
{

	// Open .tbl file
	std::ifstream inFile;
	inFile.open("lineitem-10K-rows.tbl", std::ifstream::in);
	if(!inFile)
	{
		std::cout<<"Cannot open file!!!";
		return 0;
	}		
	// Get a row
	std::string row;
	std::getline(inFile, row);
	//inFile.close();
	// Split by '|' deliminator
	std::vector<std::string> parsedRow = split(row, '|');
	
// ------------------------------------------------------------------------Initialize FlatBuffer ----------------------------------------
	//	 Create a 'FlatBufferBuilder', which will be used to create our LINEITEM FlexBuffers
	flatbuffers::FlatBufferBuilder fbbuilder(1024);
	vector<flatbuffers::Offset<Rows>> rows_vector;
	//	 Creating temp variables to throw into the fbbuilder
	int orderkey = atoi(parsedRow[0].c_str());
	int partkey = atoi(parsedRow[1].c_str());
	int suppkey = atoi(parsedRow[2].c_str());
	int linenumber = atoi(parsedRow[3].c_str());
	float quantity = stof(parsedRow[4]);
	float extendedprice = stof(parsedRow[5]);
	float discount = stof(parsedRow[6]);
	float tax = stof(parsedRow[7]);
	
	int8_t returnflag = (int8_t) atoi(parsedRow[8].c_str());
	int8_t linestatus = (int8_t) atoi(parsedRow[9].c_str());
	//	 Parsing and Serializing of Date fields	
	vector<int> shipdate, receiptdate, commitdate;
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	string shipinstruct = parsedRow[13];
	string shipmode = parsedRow[14];
	string comment = parsedRow[15];

	int rowID = 500;
// -----------------------------------------Create Row 0-------------------------------------------
	flexbuffers::Builder flx0;
	flx0.Vector([&]() {
		flx0.UInt(orderkey);
		flx0.UInt(partkey);
		flx0.UInt(suppkey);
		flx0.UInt(linenumber);
		flx0.Float(quantity);
		flx0.Float(extendedprice);
		flx0.Float(discount);
		flx0.Float(tax);
		flx0.UInt(returnflag);
		flx0.UInt(linestatus);
		flx0.Vector([&]() {
			flx0.UInt(shipdate[0]);
			flx0.UInt(shipdate[1]);
			flx0.UInt(shipdate[2]);
		});
		flx0.Vector([&]() {
			flx0.UInt(receiptdate[0]);
			flx0.UInt(receiptdate[1]);
			flx0.UInt(receiptdate[2]);
		});
		flx0.Vector([&]() {
			flx0.UInt(commitdate[0]);
			flx0.UInt(commitdate[1]);
			flx0.UInt(commitdate[2]);
		});
		flx0.String(shipinstruct);
		flx0.String(shipmode);
		flx0.String(comment);
	});
	flx0.Finish();
	// Get Pointer to FlexBuffer Row
	vector<uint8_t> flxPtr = flx0.GetBuffer();
// --------------------------------------------------Create 4MB of FlexBuffers-------------------------------------------------
	for(int i=0;i<20900;i++) {
		// Serialize buffer into a Flatbuffer::Vector
		auto flxSerial = fbbuilder.CreateVector(flxPtr);
		// Create a Row from FlexBuffer and new ID
		auto row0 = CreateRows(fbbuilder, rowID++, flxSerial);
		// Push new row onto vector of rows
		rows_vector.push_back(row0);
	}
//--------------------------------------------------------------Create FlatBuffer of FlexBuffers -------------------------------------
	// Serializing vector of Rows adds 12 bytes
	auto rows_vec = fbbuilder.CreateVector(rows_vector);
	auto table = CreateTable(fbbuilder, rows_vec);
	fbbuilder.Finish(table);

// --------------------------------------------Start Timing Rows--------------------------------------------------------------------
//
	// Get Pointer to FlatBuffer
	uint8_t *buf = fbbuilder.GetBufferPointer();
	int size = fbbuilder.GetSize();
	cout<<"Buffer Size (FlatBuffer): "<<size<<endl;
	// Check number of FlexBuffers in FlatBuffer
	volatile auto records = GetTable(buf);
	int recsCount = records->data()->size();
	cout<<"Recs Count: "<<recsCount<<endl;
	cout<<"Overhead for "<<recsCount<<" rows: "<<(size - (recsCount*flx0.GetSize()) ) / (double)recsCount<<" bytes per row"<<endl<<endl;
	// Initialize temporary variables to store FlexBuffer data
	volatile int32_t _orderkey, _partkey, _suppkey, _linenumber;
	volatile float _quantity, _extendedprice, _discount, _tax;
	volatile int8_t _returnflag, _linestatus;

	// FlexBuffers Vectors and Strings need to be Initialized to dummy values
	// 	No String() or Vector() constructors
	auto tempflxRoot = records->data()->Get(0)->rows_flexbuffer_root().AsVector();
	flexbuffers::Vector _shipdate = tempflxRoot[11].AsVector();
	flexbuffers::Vector _receiptdate = tempflxRoot[11].AsVector();
	flexbuffers::Vector _commitdate = tempflxRoot[11].AsVector();
	flexbuffers::String _shipinstruct = tempflxRoot[15].AsString();;
	flexbuffers::String _shipmode = tempflxRoot[15].AsString();
	flexbuffers::String _comment = tempflxRoot[15].AsString();

	// Setup the Timing test
	int rowNum = 20315;
	struct timeval start, end;
	double t;

	double avg = 0;
	double avg2 = 0;
	int n = 1000000;
	int n2 = 10;
	double minN = 1000000;
	double maxN = 0;
	// READ A ROW
		for(int i=0;i<n2;i++) {
			avg = 0;
			gettimeofday(&start, NULL);
			for(int i=0;i<n;i++) {
				records = GetTable(buf);
				const flatbuffers::Vector<flatbuffers::Offset<Rows>>* recs = records->data();
				auto flxRoot = recs->Get(rowNum)->rows_flexbuffer_root();
				auto rowVector = flxRoot.AsVector();
				_orderkey = rowVector[0].AsUInt32();
				_partkey = rowVector[1].AsUInt32();
				_suppkey = rowVector[2].AsUInt32();
				_linenumber = rowVector[3].AsUInt32();
				_quantity = rowVector[4].AsFloat();
				_extendedprice = rowVector[5].AsFloat();
				_discount = rowVector[6].AsFloat();
				_tax = rowVector[7].AsFloat();
				_returnflag = rowVector[8].AsUInt8();
				_linestatus = rowVector[9].AsUInt8();
				_shipdate = rowVector[10].AsVector();
				_commitdate = rowVector[11].AsVector();
				_receiptdate = rowVector[12].AsVector();
				_shipinstruct = rowVector[13].AsString();
				_shipmode = rowVector[14].AsString();
				_comment = rowVector[15].AsString();
			}
			gettimeofday(&end, NULL);
			
			t = (double) ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			avg += t;
			avg /= n;
			minN = minN<avg?minN:avg;
			maxN = maxN>avg?maxN:avg;
			avg2 += avg;
		}
		avg2 /= n2;
	std::cout<<"Reading LINEITEM took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

// ------------------------------------------------- DOUBLE CHECKING CONTENTS OF LINEITEM IN BUFFER -----------------------------------
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
