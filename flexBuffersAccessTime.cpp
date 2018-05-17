// Written by Billy Lai
// 5/15/18
// Reading and Writing LineItem FlexBuffers

#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include "flatbuffers/flexbuffers.h"

using namespace std;

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
	std::string line;
	std::getline(inFile, line);
	inFile.close();
	// Split by '|' deliminator
	std::vector<std::string> parsedRow = split(line, '|');
	
// ------------------------------------------------------------------------Initialize FlexBuffer ----------------------------------------
	
	//	 Creating temp variables to throw into the builder
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
	//	 Parsing of Date fields	
	vector<int> shipdate,receiptdate,commitdate;
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	//	 String fields
	string shipinstruct = parsedRow[13];
	string shipmode = parsedRow[14];
	string comment = parsedRow[15];
	// Initialize our LINEITEM
	// 	Create a flexbuffer as a map
	flexbuffers::Builder fbb;
	fbb.Map([&]() {
		fbb.UInt("orderkey", orderkey);
		fbb.UInt("partkey", partkey);
		fbb.UInt("suppkey", suppkey);
		fbb.UInt("linenumber", linenumber);
		fbb.Float("quantity", quantity);
		fbb.Float("extendedprice", extendedprice);
		fbb.Float("discount", discount);
		fbb.Float("tax", tax);
		fbb.UInt("returnflag", returnflag);
		fbb.UInt("linestatus", linestatus);
		fbb.Vector("shipdate", [&]() {
			fbb.UInt(shipdate[0]);
			fbb.UInt(shipdate[1]);
			fbb.UInt(shipdate[2]);
		});
		fbb.Vector("receiptdate", [&]() {
			fbb.UInt(receiptdate[0]);
			fbb.UInt(receiptdate[1]);
			fbb.UInt(receiptdate[2]);
		});
		fbb.Vector("commitdate", [&]() {
			fbb.UInt(commitdate[0]);
			fbb.UInt(commitdate[1]);
			fbb.UInt(commitdate[2]);
		});
		fbb.String("shipinstruct", shipinstruct);
		fbb.String("shipmode", shipmode);
		fbb.String("comment", comment);
	});
	fbb.Finish();
	//	Create a flexbuffer as a vector
	flexbuffers::Builder fbb2;
	fbb2.Vector([&]() {
		fbb2.UInt(orderkey);
		fbb2.UInt(partkey);
		fbb2.UInt(suppkey);
		fbb2.UInt(linenumber);
		fbb2.Float(quantity);
		fbb2.Float(extendedprice);
		fbb2.Float(discount);
		fbb2.Float(tax);
		fbb2.UInt(returnflag);
		fbb2.UInt(linestatus);
		fbb2.Vector([&]() {
			fbb2.UInt(shipdate[0]);
			fbb2.UInt(shipdate[1]);
			fbb2.UInt(shipdate[2]);
		});
		fbb2.Vector([&]() {
			fbb2.UInt(receiptdate[0]);
			fbb2.UInt(receiptdate[1]);
			fbb2.UInt(receiptdate[2]);
		});
		fbb2.Vector([&]() {
			fbb2.UInt(commitdate[0]);
			fbb2.UInt(commitdate[1]);
			fbb2.UInt(commitdate[2]);
		});
		fbb2.String(shipinstruct);
		fbb2.String(shipmode);
		fbb2.String(comment);
	});
	fbb2.Finish();

// --------------------------------------------Initialize Temp Variables--------------------------------------------------------------------
//
	// Get Buffer pointers and check size of buffers
	vector<uint8_t> buf = fbb.GetBuffer();
	int size = fbb.GetSize();
	cout<<"Buffer Size (map): "<<size<<endl;
	vector<uint8_t> buf2 = fbb2.GetBuffer();
	int size2 = fbb2.GetSize();
	cout<<"Buffer Size (vector): "<<size2<<endl<<endl;

	// Initialize temp variables for assertions
	flexbuffers::Builder temp;
	temp.Vector([&]() {
		temp.UInt(100);
		temp.String("temp");
	});
	temp.Finish();
	vector<uint8_t> tempbuf = temp.GetBuffer();
	auto vec = flexbuffers::GetRoot(tempbuf).AsVector();

	int _orderkey, _partkey, _suppkey, _linenumber;
	float _quantity, _extendedprice, _discount, _tax;
	int8_t _returnflag, _linestatus;
	flexbuffers::Vector _shipdate = vec;
	flexbuffers::Vector _commitdate = vec;
	flexbuffers::Vector _receiptdate = vec;
	flexbuffers::String _shipinstruct = vec[1].AsString();
 	flexbuffers::String _shipmode = vec[1].AsString();
	flexbuffers::String _comment = vec[1].AsString();
	
	// -------------------------------------------------Start TIMING MAP FLEXBUFFER----------------------------------------------
	struct timeval start, end;
	double t;

	auto map = flexbuffers::GetRoot(buf).AsMap();
	double avg = 0;
	double avg2 = 0;
	int n = 1000000;
	int n2 = 10;
	double minN = 1000000;
	double maxN = 0;
	// READ A ROW using MAP
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_orderkey = map["orderkey"].AsUInt32();
			_partkey = map["partkey"].AsUInt32();
			_suppkey = map["suppkey"].AsUInt32();
			_linenumber = map["linenumber"].AsFloat();
			_quantity = map["quantity"].AsFloat();
			_extendedprice = map["extendedprice"].AsFloat();
			_discount = map["discount"].AsFloat();
			_tax = map["tax"].AsFloat();
			_returnflag = map["returnflag"].AsUInt8();
			_linestatus = map["linestatus"].AsUInt8();
			_shipdate = map["shipdate"].AsVector();
			_receiptdate = map["receiptdate"].AsVector();
			_commitdate = map["commitdate"].AsVector();
			_shipinstruct = map["shipinstruct"].AsString();
			_shipmode = map["shipmode"].AsString();
			_comment = map["comment"].AsString();
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
	std::cout<<"Reading ROW (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;
	
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

	// -------------------------------------------------- START TIMING VECTOR FLEXBUFFER--------------------------------
	// READ A ROW using VECTOR
	auto row = flexbuffers::GetRoot(buf2).AsVector();
	avg = 0;
	avg2 = 0;
	minN = 10000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_orderkey = row[0].AsUInt32();
			_partkey = row[1].AsUInt32();
			_suppkey = row[2].AsUInt32();
			_linenumber = row[3].AsFloat();
			_quantity = row[4].AsFloat();
			_extendedprice = row[5].AsFloat();
			_discount = row[6].AsFloat();
			_tax = row[7].AsFloat();
			_returnflag = row[8].AsUInt8();
			_linestatus = row[9].AsUInt8();
			_shipdate = row[10].AsVector();
			_receiptdate = row[11].AsVector();
			_commitdate = row[12].AsVector();
			_shipinstruct = row[13].AsString();
			_shipmode = row[14].AsString();
			_comment = row[15].AsString();
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
	cout<<"Reading ROW (Vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	//	Check the number of fields of flexbuffer (map and vector)
	int mSize = map.size();
	int rSize = row.size();
	cout<<"Map Size: "<<mSize<<" Vector Size: "<<rSize<<endl;

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

	return 0;
}
// --------------------------------------------------------HELPER FUNCTIONS----------------------------------------------------------
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
