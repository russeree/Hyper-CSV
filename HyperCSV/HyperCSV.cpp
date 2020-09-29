// HyperCSV.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "csv_reader.h"

int main(){
    CsvReader CSV;
    CSV.setActiveFile("500000 Sales Records.csv");
    std::cout << "Russeree Ultimate CSV Reader!\n";
    return 0;
}