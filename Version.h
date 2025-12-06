// Version.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Represents a single version of a variable with timestamp and writer info
// Side effects: None - simple data class

#ifndef VERSION_H
#define VERSION_H

#include <string>

namespace RepCRec {

class Version {
public:
    int value;
    int commitTimestamp;
    std::string writerTransactionId;
    
    Version(int val, int commitTime, const std::string& writerId)
        : value(val), commitTimestamp(commitTime), writerTransactionId(writerId) {}
    
    // Default constructor for initial values (committed at time 0)
    Version(int val) : value(val), commitTimestamp(0), writerTransactionId("INITIAL") {}
};

} // namespace RepCRec

#endif // VERSION_H