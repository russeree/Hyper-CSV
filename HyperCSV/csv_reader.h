#pragma once
#include <iostream>   // Output and Debugging 
#include <Windows.h>  // Needed for memory allocation 
#include <stdint.h>   // Usefull Types - Type Handling
#include <stdlib.h>   // Standard Library - Self Explanitory 
#include <filesystem> // File System - Used for File Information and Pre-Processing Validation
#include <thread>     // Threading Tools - Used for Threaded CSV Parsing Operations
#include <string>     // String Tool - Used for File Paths 
#include <stdio.h>    // Standard IO tools used for a few of the default C++ implementations
#include <math.h>     // Used for floor functions and such

namespace fs = std::filesystem;

class CsvReader {
	//Variables to Configure Reader
public:
	uint32_t activeMaxThreads = 0; //Maximum number of reader threads to spawn. This is for large files This defaults to 100% avaible CPU threads. 
	uint64_t activeMemUse = 0;     //Active Maximum Memory to use. This defaults to 100% of available phys. 
	uint64_t fileLines = 0;        //Number of lines in a file to read. 
	char* csvData;                 //This is the array with the CSV data
	//Public Access Function List
public:
	CsvReader(void);
	uint8_t setActiveFile(std::string path);    //Sets the Active File to be Processes 
	uint8_t setMaxMemoryPercent(float percent); //Sets the Maximum Memory Usage as a percent of Available PHYS
	uint8_t preParseFile(fs::path& fpath);      //File PreParser - This does a barrage of checks on a file to ensure it is able to be processes
	uint8_t csvMemAlocation(void);              //Alocates the memory needed to stoe the CSV contents during parsing
	uint8_t memMapFile(void);                   //Map the Active File into Memory for the Big Read - USE ONLY WITH SSD - No Parameters Needed
	uint8_t memMapCopyThread(void);             //This is the thread that is spanwed to copy the file into memory for processing 
private:
	bool cur_active = FALSE;        //Is there a current active file being worked on
	fs::path curPath;               //The current Active File Path
	HANDLE curCsvFile;              //The cuttent csv FileCreateHandle
	HANDLE curCsvMapFile;           // handle for the file's memory-mapped region
	uint64_t curFileSize = 0;       //Current Number of Bytes of the Target File.
	uint64_t curTotalPages = 0;     //This is the total number of pages needed to read a filew 
	uint64_t curPageSize = 0;       //Current Block Size: A block is the largest chunk of a file to be split into threads for reading;  
	uint64_t curChunksPerPage = 0;  //This is the current number of chunks per page based on the data granulatiy 
	uint64_t curChunksPerPageRm = 0;//This it the remainder of chunks per page
	uint64_t curStartSplitSize = 0; //This is the Size of the Split Chunks 
	uint64_t curSysGranularity = 0; //This is the Current Granularity of Pages on the HDD, Used for file Mapping Support
	//Configuration Profile Overrides;
public:
	uint64_t f_max_threads = 0;  //Override Maximum Threads, This Number MAY NOT EXCEED maxThreads
	uint64_t f_max_memory = 0;   //Override Maximum Memory Usage, (!!!WARNING!!! VERY DANGEROUS)
};

/**
 * @Doc: This class takes in a file path as an active target for parsing; At this point the file is determined to need a page size; If the
 * file contains more data than that which is in system memory non-paged then a file must be divided into pages to be parsed. A page is then divided up into the
 * the number is sections to be parsed at one time. This is determined by the number of threads available or the override value. At this point worker threads can then begin
 * to process the .csv file.
 *
 * @Note: Processing the .CSV starts by taking the point of a split and determining where the first \n begins
 * @Note: This is one of the tricker portions of the the script, you as a programmer must alocate an array dynamicaly of pointers to arrays of char objects which are then filled up via the thread handlers, at this point you must
 * preserve the contents of the memory beyond the life span of the thread handlers. Then even tricker these addresses must be able to be read contigiously within the processing algorithms.
 *     - A possible solution to this problem is to alocate an array then use offsets to write to the locations
 *     - How would the above play with threading? Given that there would be many programs tryin to write to the same variable
 *     - Could you create a plethora a variables to write too to avoid concurent write operations or a mutex locking out a variable
 **/