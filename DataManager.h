// DataManager.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Manages data storage and versioning at a single site
// Inputs: Variable IDs, values, timestamps
// Outputs: Version objects for reads, void for writes
// Side effects: Modifies dataStore, writeBuffer, replicaReadEnabled state

#ifndef DATAMANAGER_H
#define DATAMANAGER_H

#include "Version.h"
#include "Constants.h"
#include <vector>
#include <map>
#include <string>
#include <memory>

namespace RepCRec {

class DataManager {
private:
    int siteId;
    bool isUp;
    int lastRecoveryTime;
    
    // Data storage: variableId -> list of versions (sorted by commitTimestamp)
    std::map<int, std::vector<Version>> dataStore;
    
    // Write buffers: transactionId -> (variableId -> value)
    std::map<std::string, std::map<int, int>> writeBuffer;
    
    // Replicated variable read gates (for post-recovery reads)
    std::map<int, bool> replicaReadEnabled;
    
public:
    DataManager(int siteId);
    
    // Initialize variables at this site
    void initialize();
    
    // Read a variable at a specific snapshot time
    // Returns nullptr if no valid version exists
    std::shared_ptr<Version> readVariable(int variableId, int snapshotTime);
    
    // Buffer a write for a transaction
    void writeVariable(int variableId, int value, const std::string& transactionId);
    
    // Commit all buffered writes for a transaction
    void commitWrites(const std::string& transactionId, int commitTimestamp);
    
    // Abort/discard buffered writes for a transaction
    void abortWrites(const std::string& transactionId);
    
    // Site failure handling
    void onFailure();
    
    // Site recovery handling
    void onRecovery(int currentTime);
    
    // Get committed state for dump
    std::map<int, int> getCommittedState() const;
    
    // Check if a replicated variable is readable
    bool isReplicaReadable(int variableId) const;
    
    // Getters
    bool getIsUp() const { return isUp; }
    int getSiteId() const { return siteId; }
    
    // Get direct access to dataStore for snapshot validation
    std::map<int, std::vector<Version>>& getDataStore() { return dataStore; }
};

} // namespace RepCRec

#endif // DATAMANAGER_H