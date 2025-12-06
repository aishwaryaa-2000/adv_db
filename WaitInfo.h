// WaitInfo.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Track information about blocked/waiting transactions
// Side effects: None - data container

#ifndef WAITINFO_H
#define WAITINFO_H

#include <set>

namespace RepCRec {

class WaitInfo {
public:
    int variableId;
    std::set<int> candidateSites;  // Sites that might become readable
    
    WaitInfo() : variableId(-1) {}
    
    WaitInfo(int varId, const std::set<int>& sites) 
        : variableId(varId), candidateSites(sites) {}
    
    void clear() {
        variableId = -1;
        candidateSites.clear();
    }
    
    bool isWaiting() const {
        return variableId != -1;
    }
};

} // namespace RepCRec

#endif // WAITINFO_H