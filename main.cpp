// main.cpp
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Main entry point for RepCRec distributed database system
// Usage: ./repcrec < input.txt

#include "TransactionManager.h"
#include "Parser.h"
#include <iostream>
#include <memory>

int main() {
    std::cout << "RepCRec - Distributed Database with SSI and Available Copies" << std::endl;
    std::cout << "=============================================================" << std::endl << std::endl;
    
    // Create transaction manager
    auto tm = std::make_shared<RepCRec::TransactionManager>();
    
    // Create parser
    RepCRec::Parser parser(tm);
    
    // Run the parser on stdin
    parser.run();
    
    std::cout << "\n=============================================================" << std::endl;
    std::cout << "RepCRec execution complete" << std::endl;
    
    return 0;
}