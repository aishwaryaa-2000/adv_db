// Transaction.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Represents a single transaction with all SSI metadata
// Side effects: Updates conflict tracking when methods are called

#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "Constants.h"
#include "WaitInfo.h"
#include "ReadInfo.h"
#include "WriteInfo.h"
#include <string>
#include <map>
#include <set>

namespace RepCRec {

class Transaction {
public:
    std::string id;
    int startTime;
    int commitTime;
    TransactionStatus status;
    
    // Read and write tracking
    std::map<int, ReadInfo> readSet;        // variableId -> ReadInfo
    std::map<int, WriteInfo> writeSet;      // variableId -> WriteInfo
    
    // Site access tracking for failure detection
    std::set<int> writeSites;                    // Sites we've written to
    std::map<int, int> firstAccessTimePerSite;   // siteId -> first access time
    std::set<int> criticalReadSites;             // Sites we read unreplicated vars from
    
    // Conflict tracking for RW-cycle detection
    std::set<std::string> incomingReadWriteConflicts;  // Txns that wrote what we read
    std::set<std::string> outgoingReadWriteConflicts;  // Txns that read what we wrote
    
    // Wait information
    WaitInfo waitInfo;
    
    Transaction(const std::string& txnId, int startT)
        : id(txnId), startTime(startT), commitTime(-1), status(TransactionStatus::ACTIVE) {}
    
    // Check if this transaction is waiting
    bool isWaiting() const {
        return status == TransactionStatus::WAITING;
    }
    
    // Add a read operation
    void addRead(int variableId, int siteId, int value, int versionTime, 
                 const std::string& writerId) {
        readSet.emplace(variableId, ReadInfo(siteId, value, versionTime, writerId));
    }
    
    // Add a write operation
    void addWrite(int variableId, int value) {
        if (writeSet.find(variableId) == writeSet.end()) {
            writeSet.emplace(variableId, WriteInfo(value));
        } else {
            writeSet.at(variableId).value = value;  // Update value
        }
    }
    
    // Add RW conflict edges
    void addIncomingRWEdge(const std::string& fromTxnId) {
        incomingReadWriteConflicts.insert(fromTxnId);
    }
    
    void addOutgoingRWEdge(const std::string& toTxnId) {
        outgoingReadWriteConflicts.insert(toTxnId);
    }
    
    // Mark transaction as waiting
    void setWaiting(int variableId, const std::set<int>& candidateSites) {
        status = TransactionStatus::WAITING;
        waitInfo = WaitInfo(variableId, candidateSites);
    }
    
    // Resume from waiting
    void resumeFromWaiting() {
        status = TransactionStatus::ACTIVE;
        waitInfo.clear();
    }
};

} // namespace RepCRec

#endif // TRANSACTION_H