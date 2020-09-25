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
		this->f_max_memory = floor((double)this->activeMemUse * (double)percent);
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
	if (this->curCsvFile == INVALID_HANDLE_VALUE) {return 1;} //Fail if the file handler creation has failed./
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

/**
 * @desc: This is the function to calculate the number of chunks(based on system granularity to grab per thread) this returns a vector with the offsets and sizes for the entire file
 * this function also serves to ensure that the generation of chunks is safe and legal compared to a user defined function within the scope of the thread worker. 
 * @note: This function should be a part of the pre-processing loop. 
 **/
uint8_t CsvReader::calcOffsetsPerThread(void){
	uint64_t currentViewWindow = 0; //Init the starting windows @ 0
	uint64_t totalBlocks = 0; //This is the total number of chunks needed to process an entire file
	/* Get the Block Size in Number of Blocks to Read - MATH*/
	totalBlocks = ceil((double)this->curFileSize / (double)this->curSysGranularity);

	return 0; 
}