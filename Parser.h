// Parser.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Parse input commands and execute them via TransactionManager
// Input: String commands from stdin
// Output: Executes commands
// Side effects: Calls TransactionManager methods

#ifndef PARSER_H
#define PARSER_H

#include "TransactionManager.h"
#include <string>
#include <memory>

namespace RepCRec {

class Parser {
private:
    std::shared_ptr<TransactionManager> tm;
    
    // Helper methods
    std::string trim(const std::string& str);
    std::vector<std::string> split(const std::string& str, char delimiter);
    
public:
    Parser(std::shared_ptr<TransactionManager> manager);
    
    // Parse and execute a single command
    void parseCommand(const std::string& line);
    
    // Run the parser on input stream
    void run();
};

} // namespace RepCRec

#endif // PARSER_H