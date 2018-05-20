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
	flatbuffers::FlatBufferBuilder builder(1024);
	vector<flatbuffers::Offset<Rows>> rows_vector;
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
	//	 Parsing and Serializing of Date fields	
	vector<int> shipdate, receiptdate, commitdate;
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	string shipinstruct = parsedRow[13];
	string shipmode = parsedRow[14];
	string comment = parsedRow[15];

	int rowID = 0;
// -----------------------------------------Create Row 0-------------------------------------------
	flexbuffers::Builder fbb0;
	fbb0.Vector([&]() {
		fbb0.UInt(orderkey);
		fbb0.UInt(partkey);
		fbb0.UInt(suppkey);
		fbb0.UInt(linenumber);
		fbb0.Float(quantity);
		fbb0.Float(extendedprice);
		fbb0.Float(discount);
		fbb0.Float(tax);
		fbb0.UInt(returnflag);
		fbb0.UInt(linestatus);
		fbb0.Vector([&]() {
			fbb0.UInt(shipdate[0]);
			fbb0.UInt(shipdate[1]);
			fbb0.UInt(shipdate[2]);
		});
		fbb0.Vector([&]() {
			fbb0.UInt(receiptdate[0]);
			fbb0.UInt(receiptdate[1]);
			fbb0.UInt(receiptdate[2]);
		});
		fbb0.Vector([&]() {
			fbb0.UInt(commitdate[0]);
			fbb0.UInt(commitdate[1]);
			fbb0.UInt(commitdate[2]);
		});
		fbb0.String(shipinstruct);
		fbb0.String(shipmode);
		fbb0.String(comment);
	});
	fbb0.Finish();
	// Get Pointer to FlexBuffer Row
	vector<uint8_t> buff = fbb0.GetBuffer();
	// Serialize buffer into a FlatBuffer::Vector
	auto fb0 = builder.CreateVector(buff);
	// Create a Row from FlexBuffer and new ID
	auto row0 = CreateRows(builder, rowID++, fb0); 
	// Push new row onto vector of rows
	rows_vector.push_back(row0);	
// --------------------------------------------------Create Row 1-------------------------------------------------
	// Get a row
	getline(inFile, row);
	// Split by '|' deliminator
	parsedRow = split(row, '|');
	orderkey = atoi(parsedRow[0].c_str());
	partkey = atoi(parsedRow[1].c_str());
	suppkey = atoi(parsedRow[2].c_str());
	linenumber = atoi(parsedRow[3].c_str());
	quantity = stof(parsedRow[4]);
	extendedprice = stof(parsedRow[5]);
	discount = stof(parsedRow[6]);
	tax = stof(parsedRow[7]);
	
	returnflag = (int8_t) atoi(parsedRow[8].c_str());
	linestatus = (int8_t) atoi(parsedRow[9].c_str());
	//	 Parsing and Serializing of Date fields	
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	shipinstruct = parsedRow[13];
	shipmode = parsedRow[14];
	comment = parsedRow[15];

	flexbuffers::Builder fbb1;
	fbb1.Vector([&]() {
		fbb1.UInt(orderkey);
		fbb1.UInt(partkey);
		fbb1.UInt(suppkey);
		fbb1.UInt(linenumber);
		fbb1.Float(quantity);
		fbb1.Float(extendedprice);
		fbb1.Float(discount);
		fbb1.Float(tax);
		fbb1.UInt(returnflag);
		fbb1.UInt(linestatus);
		fbb1.Vector([&]() {
			fbb1.UInt(shipdate[0]);
			fbb1.UInt(shipdate[1]);
			fbb1.UInt(shipdate[2]);
		});
		fbb1.Vector([&]() {
			fbb1.UInt(receiptdate[0]);
			fbb1.UInt(receiptdate[1]);
			fbb1.UInt(receiptdate[2]);
		});
		fbb1.Vector([&]() {
			fbb1.UInt(commitdate[0]);
			fbb1.UInt(commitdate[1]);
			fbb1.UInt(commitdate[2]);
		});
		fbb1.String(shipinstruct);
		fbb1.String(shipmode);
		fbb1.String(comment);
	});
	fbb1.Finish();
	// Get Pointer to FlexBuffer Row
	buff = fbb1.GetBuffer();
	// Serialize buffer into a FlatBuffer::Vector
	auto fb1 = builder.CreateVector(buff);
	// Create a Row from FlexBuffer and new ID
	auto row1 = CreateRows(builder, rowID++, fb1); 
	// Push new row onto vector of rows
	rows_vector.push_back(row1);	
//--------------------------------------------------------------Created 2 rows -------------------------------------
	auto table = CreateTable(builder, rows_vector);
	builder.Finish(table);

	uint8_t *buf = builder.GetBufferPointer();
	int size = builder.GetSize();
	cout<<"Buffer Size: "<<size<<endl<<endl;

	auto records = GetTable(buf);
	flatbuffers::Vector<flatbuffers::Offset<Rows>> recs = records->data();
	auto firstRow = flexbuffers::GetRoot(recs[0]).AsVector();
	cout<<"First Row's Orderkey: "<<firstRow[0]<<endl;

	auto record = flexbuffers::GetRoot(buff).AsUInt8();
	
/*
// --------------------------------------------Start Timing Rows--------------------------------------------------------------------
//
	struct timeval start, end;
	double t;

//	uint8_t *buf = builder.GetBufferPointer();
//	int size = builder.GetSize();
//	std::cout<<"Buffer Size (FlatBuffer): "<<size<<endl<<endl;

//	volatile auto item = GetLINEITEM(buf);

	volatile int32_t _orderkey, _partkey, _suppkey, _linenumber;
	volatile float _quantity, _extendedprice, _discount, _tax;
	volatile int8_t _returnflag, _linestatus;
	const Tables::Date* _shipdate, *_commitdate, *_receiptdate;
	const flatbuffers::String*  _shipinstruct, *_shipmode, *_comment;

	double avg = 0;
	double avg2 = 0;
	int n = 100000;
	int n2 = 10;
	double minN = 1000000;
	double maxN = 0;
	// READ A ROW
		for(int i=0;i<n2;i++) {
			avg = 0;
			gettimeofday(&start, NULL);
			for(int i=0;i<n;i++) {
				item = GetLINEITEM(buf);
				_orderkey = item->L_ORDERKEY();
				_partkey = item->L_PARTKEY();
				_suppkey = item->L_SUPPKEY();
				_linenumber = item->L_LINENUMBER();
				_quantity = item->L_QUANTITY();
				_extendedprice = item->L_EXTENDEDPRICE();
				_discount = item->L_DISCOUNT();
				_tax = item->L_TAX();
				_returnflag = item->L_RETURNFLAG();
				_linestatus = item->L_LINESTATUS();
				_shipdate = item->L_SHIPDATE();
				_commitdate = item->L_COMMITDATE();
				_receiptdate = item->L_RECEIPTDATE();
				_shipinstruct = item->L_SHIPINSTRUCT();
				_shipmode = item->L_SHIPMODE();
				_comment = item->L_COMMENT();
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

	// READ AN INT:
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int j=0;j<n2;j++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			item = GetLINEITEM(buf);
			_suppkey = item->L_SUPPKEY();
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
	cout<<"READING INT took "<<avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading INT:minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A FLOAT
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int j=0;j<n2;j++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			item = GetLINEITEM(buf);
			_quantity = item->L_QUANTITY();
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
	cout<<"READING FLOAT took "<<avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading FLOAT:minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A BYTE:
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int j=0;j<n2;j++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			item = GetLINEITEM(buf);
			_returnflag = item->L_RETURNFLAG();
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
	cout<<"READING BYTE took "<<avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading BYTE:minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;
	// READ A DATE STRUCT:
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int j=0;j<n2;j++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			item = GetLINEITEM(buf);
			_shipdate = item->L_SHIPDATE();
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
	cout<<"READING DATE took "<<avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading DATE:minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A STRING:
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int j=0;j<n2;j++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			item = GetLINEITEM(buf);
			_comment = item->L_COMMENT();
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
	cout<<"READING STRING took "<<avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading STRING:minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

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
