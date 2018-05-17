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
	
// ------------------------------------------------------------------------Initialize FlatBuffer ----------------------------------------
	
	//	 Creating temp variables to throw into the builder
	int orderkey = atoi(parsedRow[0].c_str());
	int partkey = atoi(parsedRow[1].c_str());
	int suppkey = atoi(parsedRow[2].c_str());
	int linenumber = atoi(parsedRow[3].c_str());
	double quantity = stof(parsedRow[4]);
	double extendedprice = stof(parsedRow[5]);
	double discount = stof(parsedRow[6]);
	double tax = stof(parsedRow[7]);
	
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

// --------------------------------------------Start Timing Rows--------------------------------------------------------------------
//
	struct timeval start, end;
	double t;

	vector<uint8_t> buf = fbb.GetBuffer();
	int size = fbb.GetSize();
	cout<<"Buffer Size (map): "<<size<<endl;
	vector<uint8_t> buf2 = fbb2.GetBuffer();
	int size2 = fbb2.GetSize();
	cout<<"Buffer Size (vector): "<<size2<<endl<<endl;

	auto map = flexbuffers::GetRoot(buf).AsMap();
/*
	auto _orderkey, _partkey, _suppkey, _linenumber;
	auto _quantity, _extendedprice, _discount, _tax;
	auto _returnflag, _linestatus;
	auto  _shipdate, _commitdate, _receiptdate;
	auto _shipinstruct, _shipmode, _comment;
*/
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
			auto _orderkey = map["orderkey"].AsUInt32();
			auto _partkey = map["partkey"].AsUInt32();
			auto _suppkey = map["suppkey"].AsUInt32();
			auto _linenumber = map["linenumber"].AsFloat();
			auto _quantity = map["quantity"].AsFloat();
			auto _extendedprice = map["extendedprice"].AsFloat();
			auto _discount = map["discount"].AsFloat();
			auto _tax = map["tax"].AsFloat();
			auto _returnflag = map["returnflag"].AsUInt8();
			auto _linestatus = map["linestatus"].AsUInt8();
			auto _shipdate = map["shipdate"].AsVector();
			auto _commitdate = map["commitdate"].AsVector();
			auto _receiptdate = map["receiptdate"].AsVector();
			auto _shipinstruct = map["shipinstruct"].AsString();
			auto _shipmode = map["shipmode"].AsString();
			auto _comment = map["comment"].AsString();
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
			auto _orderkey = row[0].AsUInt32();
			auto _partkey = row[1].AsUInt32();
			auto _suppkey = row[2].AsUInt32();
			auto _linenumber = row[3].AsFloat();
			auto _quantity = row[4].AsFloat();
			auto _extendedprice = row[5].AsFloat();
			auto _discount = row[6].AsFloat();
			auto _tax = row[7].AsFloat();
			auto _returnflag = row[8].AsUInt8();
			auto _linestatus = row[9].AsUInt8();
			auto _shipdate = row[10].AsVector();
			auto _commitdate = row[11].AsVector();
			auto _receiptdate = row[12].AsVector();
			auto _shipinstruct = row[13].AsString();
			auto _shipmode = row[14].AsString();
			auto _comment = row[15].AsString();
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

	int mSize = map.size();
	int rSize = row.size();
	cout<<"Map Size: "<<mSize<<" Vector Size: "<<rSize<<endl;
/*
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
*/

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
