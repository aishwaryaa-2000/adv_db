// WriteInfo.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Track information about a write operation
// Side effects: None - data container

#ifndef WRITEINFO_H
#define WRITEINFO_H

#include <set>

namespace RepCRec {

class WriteInfo {
public:
    int value;
    std::set<int> sitesApplied;  // Sites where this write was buffered
    
    WriteInfo(int val) : value(val) {}
    
    void addSite(int siteId) {
        sitesApplied.insert(siteId);
    }
};

} // namespace RepCRec

#endif // WRITEINFO_H