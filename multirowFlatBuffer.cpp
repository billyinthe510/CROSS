// Written by Billy Lai
// 5/23/18
// Reading and Writing MultiRow LineItem FlatBuffers

#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include "multirowFlatBuffer_generated.h"

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
	inFile.close();
	// Split by '|' deliminator
	std::vector<std::string> parsedRow = split(row, '|');
	
// ------------------------------------------------------------------------Initialize FlatBuffer ----------------------------------------
	const int VECTOR_SIZE = 100;
	//	 Create a 'FlatBufferBuilder', which will be used to create our LINEITEM FlatBuffers
	flatbuffers::FlatBufferBuilder fbb;
	vector<flatbuffers::Offset<Row>> rows;
	rows.reserve(VECTOR_SIZE);
	for(int i=0; i<VECTOR_SIZE;i++) {

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
	vector<int> s,r,c;
	s = splitDate(parsedRow[10]);
	r = splitDate(parsedRow[11]);
	c = splitDate(parsedRow[12]);
	auto shipdate = Date(s[0], s[1], s[2]);
	auto commitdate = Date(r[0], r[1], r[2]);
	auto receiptdate = Date(c[0], c[1], c[2]);
	//	 Serializing String fields
	auto shipinstruct = fbb.CreateString(parsedRow[13]);
	auto shipmode = fbb.CreateString(parsedRow[14]);
	auto comment = fbb.CreateString(parsedRow[15]);

	RowBuilder rb(fbb);
	rb.add_L_ORDERKEY(orderkey);
	rb.add_L_PARTKEY(partkey);
	rb.add_L_SUPPKEY(suppkey);
	rb.add_L_LINENUMBER(linenumber);
	rb.add_L_QUANTITY(quantity);
	rb.add_L_EXTENDEDPRICE(extendedprice);
	rb.add_L_DISCOUNT(discount);
	rb.add_L_TAX(tax);
	rb.add_L_RETURNFLAG(returnflag);
	rb.add_L_LINESTATUS(linestatus);
	rb.add_L_SHIPDATE(shipdate);
	rb.add_L_RECEIPTDATE(receiptdate);
	rb.add_L_COMMITDATE(commitdate);
	rb.add_L_SHIPINSTRUCT(shipinstruct);
	rb.add_L_SHIPMODE(shipmode);
	
	rows.push_back(rb.Finish());
	}
	FinishTableBuffer(fbb, CreateTable(fbb, fbb.CreateVector(rows)));
//	auto bb = fbb.ReleaseBufferPointer();
//	auto table = GetTable(bb.get());

// --------------------------------------------Start Timing Rows--------------------------------------------------------------------
//
	struct timeval start, end;
	double t;

	uint8_t *buf = fbb.GetBufferPointer();
	int size = fbb.GetSize();
	std::cout<<"Buffer Size (FlatBuffer): "<<size<<endl<<endl;

	auto table = GetTable(buf);
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
				auto item = *table->rows()->Get(0);
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
