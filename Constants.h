// Constants.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Define system-wide constants for the distributed database

#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>
#include <set>

namespace RepCRec {

// System configuration
const int NUM_VARIABLES = 20;
const int NUM_SITES = 10;
const int INITIAL_VALUE_MULTIPLIER = 10;

// Transaction status
enum class TransactionStatus {
    ACTIVE,
    WAITING,
    COMMITTED,
    ABORTED
};

// Function declarations (definitions in Constants.cpp)
bool isOddVariable(int varId);
bool isReplicatedVariable(int varId);
int getHomeSite(int varId);
std::set<int> getVariableSites(int varId);
std::string statusToString(TransactionStatus status);

} // namespace RepCRec

#endif // CONSTANTS_H