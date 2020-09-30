#include "csv_reader.h"
/**
 * @Desc: Class constructor; Detects Cores, Threads, and Relevant CPU features
 **/
	CsvReader::CsvReader(void) {
	/* Get The Available System Mememory to Use - This is to ensure the CSV parser does not attempt to grab more file than there is memory */
	MEMORYSTATUSEX statex;
	statex.dwLength = sizeof(statex);
	GlobalMemoryStatusEx(&statex);
	/* Get the block size granularity of the system that is running */
	SYSTEM_INFO SysInfo;
	GetSystemInfo(&SysInfo);
	this->curSysGranularity = SysInfo.dwAllocationGranularity;
	/* Setup Parameters */
	this->activeMaxThreads = std::thread::hardware_concurrency();
	this->activeMemUse = statex.ullAvailPhys;
}
/* Sets up the Parser with and active file */
uint8_t CsvReader::setActiveFile(std::string path) {
	this->curPath = fs::u8path(path); //Create the path object for the target 
	if (!fs::exists(this->curPath)) {
		this->cur_active = FALSE;
		return 1; //Return a Generic Failure Notice.
	};
	preParseFile(this->curPath);
	this->cur_active = TRUE;
	memMapFile();
	startCSVWorkers();
	return 0; //If all goes well then return a 0 pass. 
}
/* Sets a max memory override based on a percentage of available memory: consumes a float < 100 */
uint8_t CsvReader::setMaxMemoryPercent(float percent) {
	if (0 < percent && percent < 100) { //Check to make sure the users float percent is within a valid range of percents 
		this->f_max_memory = (uint64_t)floor((double)this->activeMemUse * (double)percent);
	}
	else{
		return 1; //Failed due to impossible bounding issues
	}
	return 0;
}
/* Pre-Parses a file for the */
uint8_t CsvReader::preParseFile(fs::path& fpath){
	/* Get the active file size */
	this->curFileSize = file_size(fpath);
	/* Determine the number of pages needed to parse the file */
	if (!this->f_max_memory)
		this->curTotalPages = (uint64_t)(ceil((double)this->curFileSize / (double)this->activeMemUse)); //file size over active memeory
	else
		this->curTotalPages = (uint64_t)(ceil((double)this->curFileSize / (double)this->f_max_memory)); //file size over active memeory for forced memory limits
	this->curPageSize = (uint64_t)(ceil((double)this->curFileSize / (double)this->curTotalPages));      //Split the file into the pages needed for memory use
	this->curChunksPerPage = (uint64_t)(ceil((double)this->curPageSize / (double)curSysGranularity));   //Find out how much totoal chunks need to be read, you will read all the chunks and there will be some overlap, but just take the byes needed from the pages. 
	/* !!!STUB!!! Determin the size of each page for the individual threads */
	return 0;
}
/**
 * @desc: This is the memory mapper, this is used to map the file on disk into memory for discontigious access
 * @warn: Use only with NVME SSD, Do not use with physical disks - performance will be bad
 **/
uint8_t CsvReader::memMapFile(void){
	if (!this->cur_active){ 
		return 1; 
	} //Failure as there is no active file to map. 
	// Acutual Memory Mapping Portion of the File Access - Read Only No Writes
	this->curCsvFile = CreateFileA(
		this->curPath.string().c_str(), //The current path
		GENERIC_READ,                   //Read the file only 
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL
	);
	if (this->curCsvFile == INVALID_HANDLE_VALUE){return 1;} //Fail if the file handler creation has failed./
	// Create the File Mapping Objects
	this->curCsvMapFile = CreateFileMappingA(
		this->curCsvFile,         // current file handle
		NULL,                     // default security
		PAGE_READONLY,            // read permission
		0,                        // size of mapping object, high
		(DWORD)this->curFileSize, // size of mapping object, low
		NULL                      // name of mapping object
	);
	// Check if the CSV file was mapped 
	if (this->curCsvMapFile == NULL)
		return 1;
	//Memory Allocation Phase -Alocate the Memory Requied for the Page Being Read 
	calcOffsetsPerThread();
	csvMemAlocation();
	return 0;
}
/**
 * @desc: Create a vector loaded up with thread objects to be used for processing work.
 **/
uint8_t CsvReader::startCSVWorkers(void) {
	/* Create an array of threads to be worked on */
	for (uint64_t task = 0; task < this->readOffsets.size(); task++) {
		this->threads.push_back(std::thread(&CsvReader::memMapCopyThread, this, task));
		this->threads.back().join();
	}
	/*
	for (int i = 0; i < this->curFileSize; i++)
		std::cout << this->csvData[i];
	*/
	FILE* pFile;
	fopen_s(&pFile, "file.csv", "wb");
	fwrite(this->csvData, 1, this->curFileSize * sizeof(char), pFile);
	fclose(pFile);
	return 0; //The function has completed successfuly 
}
/**
 * @req: Run as a seperate thread. 
 * @desc: This is the actual read thread to the memory mapped file.
 **/ 
void CsvReader::memMapCopyThread(uint64_t taskIndex){
	LPVOID lpMapAddress;
	lpMapAddress = MapViewOfFile(this->curCsvMapFile, FILE_MAP_READ, 0, this->readOffsets[taskIndex].address_start, this->readOffsets[taskIndex].bytes_to_map); //Map the view window
	if (lpMapAddress != NULL){
		char* pData;
		pData = (char*)lpMapAddress;
		memcpy(&this->csvData[this->readOffsets[taskIndex].address_start], pData, this->readOffsets[taskIndex].bytes_to_map);
		this->readOffsets[taskIndex].processed = TRUE; //The task has been completed. 
	}
}
 /**
  * @desc: Calculates and Alocates the Memory neccesary to parse the CSV files
  **/
uint8_t CsvReader::csvMemAlocation(void) {
	this->csvData = (char*)malloc(this->curPageSize);
	return 0;
}
/***
 * @desc: This is the function to calculate the number of chunks(based on system granularity to grab per thread) this returns a vector with the offsets and sizes for the entire file
 * this function also serves to ensure that the generation of chunks is safe and legal compared to a user defined function within the scope of the thread worker. 
 * @note: This function should be a part of the pre-processing loop. 
 ***/
uint8_t CsvReader::calcOffsetsPerThread(void){
	//this->activeMemUse = 200000; // Testing 
	uint64_t partialBytesNeeded  = 0; //When converted to blocks how many bytes are left over to process 
	uint64_t fullBlocksNeeded    = 0; //This is the total number of chunks needed to process an entire file
	uint64_t totalBlocksInMemory = 0; //This is the total number of blocks that can be placed into the alocated memory
	uint64_t totalPagesNeeded    = 0; //This is the total number of pages needed to complete the job, A.K.A the total number of times your active memory would need to be filled to 100%
	uint64_t lastBlockClipping   = 0; //This is the total number of bytes left over in the last block, if the file is shorter than the total number of blocks when viewed as bytes 
	/* This section is for multi-paged files that can not fit into memory */
	uint64_t numberOfBlocksPerPage   = 0;   //This is the block size per page 
	uint64_t remainderOfBlocksNeeded = 0;   //This is the number of blocks on the last page as the file is likely to use an exact multiple of of the ammount of ram alocated to the reading threads 
	/* Get the Block Size in Number of Blocks to Read - MATH Total*/
	fullBlocksNeeded    = (uint64_t)(this->curFileSize / this->curSysGranularity);     //Always add +1 block to the end unless it divides perfectly
	partialBytesNeeded  = (uint64_t)(this->curFileSize % this->curSysGranularity);     //This is the number of left over bytes that need to be oricessed 
	totalBlocksInMemory = (uint64_t)(this->activeMemUse / this->curSysGranularity);    //Calc the number of blocks that will fit in the active available memory space
	totalPagesNeeded    = (uint64_t)(fullBlocksNeeded / totalBlocksInMemory);          //Calc the total number of times the active memory work area would be filled up completely)
	/* If the total pages needed are greater than 1, find the best structure for the memory to be alocated and utilized */
	uint64_t q = (uint64_t)((uint64_t)fullBlocksNeeded / (uint64_t)this->activeMaxThreads);       /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
	uint64_t r = (uint64_t)ceil((uint64_t)fullBlocksNeeded % (uint64_t)this->activeMaxThreads);   /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
	// Modify Q and R values if you have more than one page becuase of the way memory is going to be managed
	if (totalPagesNeeded - 1){                                                  //The sub 1 is to ensure the math is done only if there is a condition where you need more than 1 page to parse the entire file
		numberOfBlocksPerPage   = this->activeMemUse / this->curSysGranularity; //This is the max number of blocks you can fit into your alocated memory: this does memory byes to blocks conversion
		remainderOfBlocksNeeded = this->activeMemUse % numberOfBlocksPerPage;   //This is how many blocks must be processed on the final page.
		uint64_t q = (uint64_t)((uint64_t)totalBlocksInMemory / (uint64_t)this->activeMaxThreads);     /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
		uint64_t r = (uint64_t)ceil((uint64_t)totalBlocksInMemory % (uint64_t)this->activeMaxThreads); /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
	}
	/* Now process the the pages and build the  vector table*/
	for(uint64_t page = 0; page < totalPagesNeeded; page++){                                                           
		/* Calculate the number of blocks each thread will process issues that araise may be the need to process remainder data when the threads dont evenly map into the memory space 1:1 */
		for (uint64_t thread = 0; thread < this->activeMaxThreads; thread++){   //use some of that y=mx+b action here to process all the data  !!!TESTING!!!
			FileOffeset threadWork;                                             //This will be creaded in order to be added to the vector of work to be done, a queue will not be used becuase of the need for out of order execution - fix in v2.0
			/* Thread Worker Job Vector Creation */
			threadWork.block_number = (page * this->activeMaxThreads) + thread; //Block Number is assigned to the current thread
			threadWork.processed = FALSE;                                       //These Blocks have not been processed yet, just generated
			threadWork.error = 0;                                               //0 means there were no errors processing the CSV data -> This will get modified by the worker thread. 
			threadWork.address_start = (DWORD)(((numberOfBlocksPerPage * page) + (thread * q)) * this->curSysGranularity);  //This is kind of a mess but should work okay. 
			threadWork.bytes_to_map = q * this->curSysGranularity;
			if (thread == (this->activeMaxThreads - 1)){
				threadWork.bytes_to_map += r * this->curSysGranularity; //Just make sure to add in the remaining bytes for the last thread worker. 
			}
			/* Push out the workers Job */
			if (threadWork.bytes_to_map) {
				this->readOffsets.push_back(threadWork); //Added your payload to the final vector!
			}
		}
	}
	this->readOffsets.back().bytes_to_map = ((q*r) * this->curSysGranularity) + (this->curFileSize % (this->curSysGranularity * r)); //This is the final thread worker - can be better but it should work just fine. 
	int x = this->readOffsets.back().bytes_to_map + this->readOffsets.back().address_start;
	return 0;
}