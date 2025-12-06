// Constants.cpp
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Implementation of utility functions for the distributed database

#include "Constants.h"

namespace RepCRec {

bool isOddVariable(int varId) {
    return varId % 2 == 1;
}

bool isReplicatedVariable(int varId) {
    return varId % 2 == 0;
}

int getHomeSite(int varId) {
    return 1 + (varId % NUM_SITES);
}

std::set<int> getVariableSites(int varId) {
    std::set<int> sites;
    if (isOddVariable(varId)) {
        sites.insert(getHomeSite(varId));
    } else {
        for (int i = 1; i <= NUM_SITES; i++) {
            sites.insert(i);
        }
    }
    return sites;
}

std::string statusToString(TransactionStatus status) {
    switch(status) {
        case TransactionStatus::ACTIVE: return "ACTIVE";
        case TransactionStatus::WAITING: return "WAITING";
        case TransactionStatus::COMMITTED: return "COMMITTED";
        case TransactionStatus::ABORTED: return "ABORTED";
    }
    return "UNKNOWN";
}

} // namespace RepCRec