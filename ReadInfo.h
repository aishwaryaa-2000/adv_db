// ReadInfo.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Track information about a read operation for RW conflict detection
// Side effects: None - data container

#ifndef READINFO_H
#define READINFO_H

#include <string>

namespace RepCRec {

class ReadInfo {
public:
    int siteId;
    int value;
    int versionTimestamp;
    std::string writerTransactionId;
    
    ReadInfo(int site, int val, int versionTime, const std::string& writerId)
        : siteId(site), value(val), versionTimestamp(versionTime), 
          writerTransactionId(writerId) {}
};

} // namespace RepCRec

#endif // READINFO_H