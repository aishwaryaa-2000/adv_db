// DataManager.cpp
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Implementation of DataManager methods

#include "DataManager.h"
#include <algorithm>
#include <iostream>

namespace RepCRec {

DataManager::DataManager(int id) 
    : siteId(id), isUp(true), lastRecoveryTime(0) {
    initialize();
}

void DataManager::initialize() {
    // Initialize all variables that belong to this site
    for (int varId = 1; varId <= NUM_VARIABLES; varId++) {
        std::set<int> sites = getVariableSites(varId);
        
        if (sites.find(siteId) != sites.end()) {
            // This site stores this variable
            int initialValue = varId * INITIAL_VALUE_MULTIPLIER;
            dataStore[varId].push_back(Version(initialValue));
            
            // Replicated variables start as readable
            if (isReplicatedVariable(varId)) {
                replicaReadEnabled[varId] = true;
            }
        }
    }
}

std::shared_ptr<Version> DataManager::readVariable(int variableId, int snapshotTime) {
    // Check if variable exists at this site
    if (dataStore.find(variableId) == dataStore.end()) {
        return nullptr;
    }
    
    // Check if replicated variable is readable after recovery
    if (isReplicatedVariable(variableId) && !replicaReadEnabled[variableId]) {
        return nullptr;  // Cannot read until post-recovery commit
    }
    
    // Find the latest version committed before or at snapshotTime
    auto& versions = dataStore[variableId];
    std::shared_ptr<Version> result = nullptr;
    
    for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
        if (it->commitTimestamp <= snapshotTime) {
            result = std::make_shared<Version>(*it);
            break;
        }
    }
    
    return result;
}

void DataManager::writeVariable(int variableId, int value, const std::string& transactionId) {
    // Buffer the write - don't modify dataStore until commit
    writeBuffer[transactionId][variableId] = value;
}

void DataManager::commitWrites(const std::string& transactionId, int commitTimestamp) {
    if (writeBuffer.find(transactionId) == writeBuffer.end()) {
        return;  // No writes to commit
    }
    
    auto& writes = writeBuffer[transactionId];
    
    for (const auto& [variableId, value] : writes) {
        // Add new version to dataStore
        dataStore[variableId].push_back(Version(value, commitTimestamp, transactionId));
        
        // If this is a replicated variable and we've recovered,
        // enable reads after this first commit
        if (isReplicatedVariable(variableId)) {
            replicaReadEnabled[variableId] = true;
        }
    }
    
    // Clear the write buffer for this transaction
    writeBuffer.erase(transactionId);
}

void DataManager::abortWrites(const std::string& transactionId) {
    // Simply discard buffered writes
    writeBuffer.erase(transactionId);
}

void DataManager::onFailure() {
    isUp = false;
    
    // Clear all write buffers (in-flight uncommitted writes are lost)
    writeBuffer.clear();
    
    // Committed data remains intact in dataStore
}

void DataManager::onRecovery(int currentTime) {
    isUp = true;
    lastRecoveryTime = currentTime;
    
    // Disable reads for replicated variables until next commit
    for (int varId = 1; varId <= NUM_VARIABLES; varId++) {
        if (isReplicatedVariable(varId) && 
            dataStore.find(varId) != dataStore.end()) {
            replicaReadEnabled[varId] = false;
        }
    }
    
    // Non-replicated variables are immediately readable
}

std::map<int, int> DataManager::getCommittedState() const {
    std::map<int, int> state;
    
    for (const auto& [variableId, versions] : dataStore) {
        if (!versions.empty()) {
            // Get the latest committed version
            state[variableId] = versions.back().value;
        }
    }
    
    return state;
}

bool DataManager::isReplicaReadable(int variableId) const {
    if (!isReplicatedVariable(variableId)) {
        return true;  // Non-replicated variables are always readable when site is up
    }
    
    auto it = replicaReadEnabled.find(variableId);
    if (it == replicaReadEnabled.end()) {
        return false;  // Variable not at this site
    }
    
    return it->second;
}

} // namespace RepCRec