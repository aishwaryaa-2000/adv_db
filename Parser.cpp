// Parser.cpp
// Author: RepCRec Team
// Date: December 2024
// Purpose: Implementation of command parser

#include "Parser.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>

namespace RepCRec {

Parser::Parser(std::shared_ptr<TransactionManager> manager) : tm(manager) {}

std::string Parser::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

std::vector<std::string> Parser::split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(trim(token));
    }
    return tokens;
}

void Parser::parseCommand(const std::string& line) {
    std::string trimmedLine = trim(line);
    
    // Skip empty lines and comments
    if (trimmedLine.empty() || trimmedLine[0] == '/' || trimmedLine[0] == '#') {
        return;
    }
    
    // Find the command name and arguments
    size_t openParen = trimmedLine.find('(');
    size_t closeParen = trimmedLine.find(')');
    
    if (openParen == std::string::npos) {
        std::cout << "Error: Invalid command format: " << trimmedLine << std::endl;
        return;
    }
    
    std::string command = trim(trimmedLine.substr(0, openParen));
    std::string argsStr;
    
    if (closeParen != std::string::npos) {
        argsStr = trimmedLine.substr(openParen + 1, closeParen - openParen - 1);
    }
    
    std::vector<std::string> args = split(argsStr, ',');
    
    // Execute commands
    if (command == "begin") {
        if (args.size() != 1) {
            std::cout << "Error: begin requires 1 argument" << std::endl;
            return;
        }
        tm->begin(args[0]);
    }
    else if (command == "R") {
        if (args.size() != 2) {
            std::cout << "Error: R requires 2 arguments" << std::endl;
            return;
        }
        std::string txnId = args[0];
        std::string varStr = args[1];
        
        // Extract variable number from "x4" or "4"
        int varId;
        if (varStr[0] == 'x') {
            varId = std::stoi(varStr.substr(1));
        } else {
            varId = std::stoi(varStr);
        }
        
        tm->read(txnId, varId);
    }
    else if (command == "W") {
        if (args.size() != 3) {
            std::cout << "Error: W requires 3 arguments" << std::endl;
            return;
        }
        std::string txnId = args[0];
        std::string varStr = args[1];
        int value = std::stoi(args[2]);
        
        // Extract variable number
        int varId;
        if (varStr[0] == 'x') {
            varId = std::stoi(varStr.substr(1));
        } else {
            varId = std::stoi(varStr);
        }
        
        tm->write(txnId, varId, value);
    }
    else if (command == "end") {
        if (args.size() != 1) {
            std::cout << "Error: end requires 1 argument" << std::endl;
            return;
        }
        tm->end(args[0]);
    }
    else if (command == "fail") {
        if (args.size() != 1) {
            std::cout << "Error: fail requires 1 argument" << std::endl;
            return;
        }
        int siteId = std::stoi(args[0]);
        tm->fail(siteId);
    }
    else if (command == "recover") {
        if (args.size() != 1) {
            std::cout << "Error: recover requires 1 argument" << std::endl;
            return;
        }
        int siteId = std::stoi(args[0]);
        tm->recover(siteId);
    }
    else if (command == "dump") {
        tm->dump();
    }
    else {
        std::cout << "Error: Unknown command: " << command << std::endl;
    }
}

void Parser::run() {
    std::string line;
    while (std::getline(std::cin, line)) {
        parseCommand(line);
    }
}

} // namespace RepCRec