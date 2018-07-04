How to Use the Client App to Flush Data to Ceph
-----------------------------------------------

Requirements
1. Install Ceph dependencies/libraries
2. Install FlatBuffers library including the flatc compiler
	github.com/google/flatbuffers
3. Client app and schema files from SkyHookDB repository
	github.com/billyinthe510/CROSS (will be updated)
4. cmake to generate our flatc compiler

Instructions
1. Get a Ceph cluster working
2. Use the .fbs schema file in the SkyHookDB repo or create your own schema file using the tutorial found at the following link:
	https://google.github.io/flatbuffers/flatbuffers_guide_tutorial.html
3. Compile the flatc.exe by following these instructions
	-git clone https://github.com/google/flatbuffers
	-cd flatbuffers
	-cmake -G "Unix Makefiles"
	-make
	-./flattests #this should print "ALL TESTS PASSED
	- now we have our ./flatc compiler
4. Compile the .fbs schema file using flatc compiler
	- This will generate a "..._generated.h"
5. Include the header files in the client app
	-"/flatbuffers/flexbuffers.h"
	-"..._generated.h" in our case ("skyhookv1_generated.h")
6. Compile client app "writeBuffer.cpp"
	- use the following command:
		g++ -W -Wall -std=c++11 writeBuffer.cpp -o writeBuffer
7. Use command line options or -h for info
	./writeBuffer -h
	./writeBuffer -f lineitem-10K-rows.tbl -s lineitem_schema.txt -o 39 -r 10 -n 50

