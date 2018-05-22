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
cout<<"fb0 size: "<<flx0.GetSize()<<endl;
	// Serialize buffer into a FlatBuffer::Vector
	auto flxSerial = builder.CreateVector(flxPtr);
	// Create a Row from FlexBuffer and new ID
	auto row0 = CreateRows(builder, rowID++, flxSerial); 
	// Push new row onto vector of rows
	rows_vector.push_back(row0);	
// --------------------------------------------------Create Row 1-------------------------------------------------
	// Get a row
	getline(inFile, row);
	// Split by '|' deliminator
	parsedRow = split(row, '|');
	orderkey = atoi(parsedRow[0].c_str());
orderkey = 55;
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

	flexbuffers::Builder flx1;
	flx1.Vector([&]() {
		flx1.UInt(orderkey);
		flx1.UInt(partkey);
		flx1.UInt(suppkey);
		flx1.UInt(linenumber);
		flx1.Float(quantity);
		flx1.Float(extendedprice);
		flx1.Float(discount);
		flx1.Float(tax);
		flx1.UInt(returnflag);
		flx1.UInt(linestatus);
		flx1.Vector([&]() {
			flx1.UInt(shipdate[0]);
			flx1.UInt(shipdate[1]);
			flx1.UInt(shipdate[2]);
		});
		flx1.Vector([&]() {
			flx1.UInt(receiptdate[0]);
			flx1.UInt(receiptdate[1]);
			flx1.UInt(receiptdate[2]);
		});
		flx1.Vector([&]() {
			flx1.UInt(commitdate[0]);
			flx1.UInt(commitdate[1]);
			flx1.UInt(commitdate[2]);
		});
		flx1.String(shipinstruct);
		flx1.String(shipmode);
		flx1.String(comment);
	});
	flx1.Finish();
	// Get Pointer to FlexBuffer Row -- vector<uint8_t>
	flxPtr = flx1.GetBuffer();
	cout<<"fb1 size: "<<flx1.GetSize()<<endl;
	// Serialize buffer into a FlatBuffer::Vector increases buffer by 190 bytes
	flxSerial = builder.CreateVector(flxPtr);
	// Create a Row from FlexBuffer and new ID add 20 bytes
	auto row1 = CreateRows(builder, rowID++, flxSerial); 
	// Push new row onto vector of rows adds 4 bytes
	rows_vector.push_back(row1);	
//--------------------------------------------------------------Created 2 rows -------------------------------------
	// Serializing vector of Rows adds 12 bytes
	auto rows_vec = builder.CreateVector(rows_vector);
	auto table = CreateTable(builder, rows_vec);
	builder.Finish(table);

	uint8_t *buf = builder.GetBufferPointer();
	int size = builder.GetSize();
	cout<<"Buffer Size: "<<size<<endl<<endl;

	int rowNum = 1;

	auto records = GetTable(buf);
	const flatbuffers::Vector<flatbuffers::Offset<Rows>>* recs = records->data();
	auto rowPtr = recs->Get(rowNum)->rows();
	int recsSize = recs->size();
	cout<<"Recs Size: "<<recsSize<<endl;

	auto f = recs->Get(rowNum)->rows_flexbuffer_root();
	auto rowVector = f.AsVector();
	auto rowOrderKey = rowVector[0].AsUInt32();
	cout<<"Row's Order Key: "<<rowOrderKey<<endl;

	auto rowLength = rowPtr->Length();
	cout<<"Row "<<rowNum<<" Flexbuffer length: "<<rowLength<<endl;
	cout<<"Row "<<rowNum<<" ID: "<<recs->Get(rowNum)->ID()<<endl;
//	cout<<"First Row's Orderkey: "<<firstFb<<endl;
	cout<<"Orderkey should be: "<<orderkey<<endl;
//	auto record = flexbuffers::GetRoot(buff).AsUInt8();
	
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
