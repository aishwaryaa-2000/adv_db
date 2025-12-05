// TransactionManager.h
// Author: RepCRec Team
// Date: December 2024
// Purpose: Central coordinator for all transactions and sites
// Manages SSI, Available Copies, and failure/recovery

#ifndef TRANSACTIONMANAGER_H
#define TRANSACTIONMANAGER_H

#include "Transaction.h"
#include "DataManager.h"
#include "SiteState.h"
#include "Constants.h"
#include <map>
#include <vector>
#include <memory>
#include <set>
#include <string>

namespace RepCRec {

class TransactionManager {
private:
    int currentTimestamp;
    std::map<std::string, std::shared_ptr<Transaction>> transactions;
    std::vector<std::shared_ptr<Transaction>> committedTransactions;
    std::map<int, std::shared_ptr<DataManager>> dataManagers;  // siteId -> DataManager
    std::map<int, SiteState> siteStates;  // siteId -> SiteState
    
    // Track commit history per variable for FCW
    std::map<int, std::vector<std::pair<std::string, int>>> variableCommitHistory;
    // variableId -> list of (transactionId, commitTime)
    
public:
    TransactionManager();
    
    // Core transaction operations
    void begin(const std::string& transactionId);
    void read(const std::string& transactionId, int variableId);
    void write(const std::string& transactionId, int variableId, int value);
    void end(const std::string& transactionId);
    
    // Site management
    void fail(int siteId);
    void recover(int siteId);
    
    // Query operations
    void dump();
    
private:
    // Read operations
    void readFromHomeSite(std::shared_ptr<Transaction> txn, int variableId);
    void readReplicated(std::shared_ptr<Transaction> txn, int variableId);
    
    // Validation at commit time
    bool violatesFailureRule(std::shared_ptr<Transaction> txn);
    bool violatesFirstCommitterWins(std::shared_ptr<Transaction> txn);
    bool violatesReadWriteCycle(std::shared_ptr<Transaction> txn);
    
    // Site availability checking
    std::set<int> computeValidSnapshotSites(std::shared_ptr<Transaction> txn, int variableId);
    bool wasSiteUpContinuously(int siteId, int fromTime, int toTime);
    
    // Cycle detection helpers
    void createRWEdgesForCommit(std::shared_ptr<Transaction> txn);  // NEW: Creates RW edges at commit time
    bool hasRWCyclePath(const std::string& fromTxnId, const std::string& targetTxnId, 
                        int edgeCount, std::set<std::string>& visited);
    bool hasPathViaRW(const std::string& fromTxnId, const std::string& toTxnId);
    
    // Commit/abort operations
    void commit(std::shared_ptr<Transaction> txn);
    void abort(std::shared_ptr<Transaction> txn, const std::string& reason);
    
    // Recovery helpers
    void retryWaitingTransactions(int recoveredSiteId);
    
    // Output helpers
    void printTransactionOutput(const std::string& transactionId, bool committed, 
                               const std::string& reason = "");
};

} // namespace RepCRec

#endif // TRANSACTIONMANAGER_H