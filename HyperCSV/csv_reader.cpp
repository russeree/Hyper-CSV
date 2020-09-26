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
	memMapCopyThread();
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
		this->curTotalPages = ceil((double)this->curFileSize / (double)this->activeMemUse); //file size over active memeory
	else
		this->curTotalPages = ceil((double)this->curFileSize / (double)this->f_max_memory); //file size over active memeory for forced memory limits
	this->curPageSize = ceil((double)this->curFileSize / (double)this->curTotalPages);      //Split the file into the pages needed for memory use
	this->curChunksPerPage = ceil((double)this->curPageSize / (double)curSysGranularity);   //Find out how much totoal chunks need to be read, you will read all the chunks and there will be some overlap, but just take the byes needed from the pages. 
	/* !!!STUB!!! Determin the size of each page for the individual threads */
	return 0;
}

/**
 * @desc: This is the memory mapper, this is used to map the file on disk into memory for discontigious access
 * @warn: Use only with NVME SSD, Do not use with physical disks - performance will be bad
 **/
uint8_t CsvReader::memMapFile(void) {
	if (!this->cur_active) { return 1; } //Failure as there is no active file to map. 
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
		this->curCsvFile,    // current file handle
		NULL,                // default security
		PAGE_READONLY,       // read permission
		0,                   // size of mapping object, high
		630110,              // size of mapping object, low
		NULL                 // name of mapping object
	);
	// Check if the CSV file was mapped 
	if (this->curCsvMapFile == NULL)
		return 1;
	//Memory Allocation Phase -Alocate the Memory Requied for the Page Being Read 
	csvMemAlocation();
	return 0;
}

/**
 * @req: Run as a seperate thread. 
 * @desc: This is the actual read thread to the memory mapped file.
 **/ 
uint8_t CsvReader::memMapCopyThread(void){
	LPVOID lpMapAddress;
	DWORD dwFileMapStart= (270000 / this->curSysGranularity) * this->curSysGranularity;
	// Now Map the View and Test the Results
	lpMapAddress = MapViewOfFile(
		this->curCsvMapFile,  // handle to mapping object
		FILE_MAP_READ,        // read/write
		0,                    // high-order 32 bits of file offset
		dwFileMapStart,       // low-order 32 bits of file offset
		100000                // number of bytes to map
	);
	if (lpMapAddress == NULL){ return 1; }
	char* pData;
	pData = (char*)lpMapAddress;
	// Extract the data, an int. Cast the pointer pData from a "pointer
	// to char" to a "pointer to int" to get the whole thing
	char iData;
	iData = *(int*)pData;
	calcOffsetsPerThread();
	return 0;
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
	this->activeMemUse = 100000;
	uint64_t currentViewWindow = 0;       //Init the starting windows @ 0
	uint64_t totalBlocks = 0;             //This is the total number of chunks needed to process an entire file
	uint64_t totalBlocksInMemory = 0;     //This is the total number of blocks that can be placed into the alocated memory
	uint64_t totalPagesNeeded = 0;        //This is the total number of pages needed to complete the job, A.K.A the total number of times your active memory would need to be filled to 100%
	uint64_t lastBlockClipping = 0;       //This is the total number of bytes left over in the last block, if the file is shorter than the total number of blocks when viewed as bytes 
	/* This section is for multi-paged files that can not fit into memory */
	uint64_t numberOfBlocksPerPage = 0;   //This is the block size per page 
	uint64_t remainderOfBlocksNeeded = 0; //This is the number of blocks on the last page as the file is likely to use an exact multiple of of the ammount of ram alocated to the reading threads 
	/* Get the Block Size in Number of Blocks to Read - MATH Total*/
	totalBlocks         = (uint64_t)ceil((double)this->curFileSize / (double)this->curSysGranularity);           //Always add +1 block to the end unless it divides perfectly
	totalBlocksInMemory = (uint64_t)floor((double)this->activeMemUse / (double)this->curSysGranularity);         //Calc the number of blocks that will fit in the active available memory space
	totalPagesNeeded    = (uint64_t)ceil((double)totalBlocks / (double)totalBlocksInMemory);                     //Calc the total number of times the active memory work area would be filled up completely
	lastBlockClipping   = (uint64_t)ceil(this->curFileSize % this->curSysGranularity);                           //Calc the number of bytes to read in the final block
	/* If the total pages needed are greater than 1, find the best structure for the memory to be alocated and utilized */
	if (totalPagesNeeded - 1){                                                //The sub 1 is to ensure the math is done only if there is a condition where you need more than 1 page to parse the entire file
		numberOfBlocksPerPage = this->activeMemUse / this->curSysGranularity; //This is the max number of blocks you can fit into your alocated memory: this does memory byes to blocks conversion
		remainderOfBlocksNeeded = this->activeMemUse % numberOfBlocksPerPage; //This is how many blocks must be processed on the final page.
	}
	/* Now process the the pages and build the  vector table*/
	for(int page = 0; page < totalPagesNeeded; page++){
		/* Thead Level Calculations for memory Access - This is the process of stitching the file together */                                                                        //There is only one page to acount for and all data can fit into memory so just work out the thread level details. 
		/* Calculate the number of blocks each thread will process issues that araise may be the need to process remainder data when the threads dont evenly map into the memory space 1:1 */
		uint64_t q = (uint64_t)((uint64_t)totalBlocks / (uint64_t)this->activeMaxThreads);     /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
		uint64_t r = (uint64_t)ceil((uint64_t)totalBlocks % (uint64_t)this->activeMaxThreads); /*!!!FIXME!!! Need to determine the number to divide when you have multiple pages*/
		/* I think the best way to handle this with regards to file operations would be to add the remaining blocks to one thread? or you could distribute the work out evenly amongst the other threads*/
		for (int thread = 0; thread < this->activeMaxThreads; thread++) {       //use some of that y=mx+b action here to process all the data  !!!TESTING!!!
			FileOffeset threadWork;                                             //This will be creaded in order to be added to the vector of work to be done, a queue will not be used becuase of the need for out of order execution - fix in v2.0
			DWORD dwFileMapStart;                                               //This is the data window to where the file mapping will start
			/* Thread Worker Job Vector Creation */
			threadWork.block_number = page * this->activeMaxThreads + thread;   //Block Number is assigned to the current thread
			threadWork.processed = FALSE;                                       //These Blocks have not been processed yet, just generated
			threadWork.error = 0;                                               //0 means there were no errors processing the CSV data -> This will get modified by the worker thread. 
			threadWork.address_start = ((numberOfBlocksPerPage * page) + (thread * q)) * this->curSysGranularity;  //This is kind of a mess but should work okay. 
			threadWork.bytesToMap = q * this->curSysGranularity;
			if (thread == (this->activeMaxThreads - 1)){
				threadWork.bytesToMap += r * this->curSysGranularity; //Just make sure to add in the remaining bytes for the last thread worker. 
			}
			/* Push out the workers Job */
			this->readOffsets.push_back(threadWork); //Added your payload to the final vector!
		}
	}
	return 0;
}