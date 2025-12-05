// TransactionManager.cpp - Complete Implementation
// Author: RepCRec Team
// Date: December 2024
// Purpose: Full implementation of TransactionManager
// Note: Save this as TransactionManager.cpp (combines all 3 parts)

#include "TransactionManager.h"
#include <iostream>
#include <algorithm>
#include <queue>

namespace RepCRec {

// ============================================================================
// CONSTRUCTOR & BASIC OPERATIONS
// ============================================================================

TransactionManager::TransactionManager() : currentTimestamp(0) {
    for (int i = 1; i <= NUM_SITES; i++) {
        dataManagers[i] = std::make_shared<DataManager>(i);
        siteStates[i] = SiteState(i);
    }
}

void TransactionManager::begin(const std::string& transactionId) {
    currentTimestamp++;
    auto txn = std::make_shared<Transaction>(transactionId, currentTimestamp);
    transactions[transactionId] = txn;
    std::cout << "Transaction " << transactionId << " begins at time " 
              << currentTimestamp << std::endl;
}

// ============================================================================
// READ OPERATIONS
// ============================================================================

void TransactionManager::read(const std::string& transactionId, int variableId) {
    currentTimestamp++;
    
    auto it = transactions.find(transactionId);
    if (it == transactions.end()) {
        std::cout << "Error: Transaction " << transactionId << " not found" << std::endl;
        return;
    }
    
    auto txn = it->second;
    
    if (txn->isWaiting()) {
        std::cout << "Transaction " << transactionId << " is waiting" << std::endl;
        return;
    }
    
    // Read-your-own-write
    if (txn->writeSet.find(variableId) != txn->writeSet.end()) {
        int value = txn->writeSet.at(variableId).value;
        std::cout << "x" << variableId << ": " << value << " (RYOW)" << std::endl;
        return;
    }
    
    if (isOddVariable(variableId)) {
        readFromHomeSite(txn, variableId);
    } else {
        readReplicated(txn, variableId);
    }
}

void TransactionManager::readFromHomeSite(std::shared_ptr<Transaction> txn, int variableId) {
    int homeSite = getHomeSite(variableId);
    
    if (!siteStates[homeSite].isUp) {
        std::cout << "Transaction " << txn->id << " waits (site " << homeSite 
                  << " down)" << std::endl;
        std::set<int> candidateSites = {homeSite};
        txn->setWaiting(variableId, candidateSites);
        return;
    }
    
    auto version = dataManagers[homeSite]->readVariable(variableId, txn->startTime);
    
    if (!version) {
        std::cout << "Error: No version for x" << variableId << std::endl;
        return;
    }
    
    txn->addRead(variableId, homeSite, version->value, 
                 version->commitTimestamp, version->writerTransactionId);
    txn->criticalReadSites.insert(homeSite);
    
    // ✅ FIX: Track first access time for this site
    if (txn->firstAccessTimePerSite.find(homeSite) == txn->firstAccessTimePerSite.end()) {
        txn->firstAccessTimePerSite[homeSite] = currentTimestamp;
    }
    
    // Track RW edge
    if (version->writerTransactionId != "INITIAL" && 
        version->writerTransactionId != txn->id) {
        auto writerIt = transactions.find(version->writerTransactionId);
        if (writerIt != transactions.end()) {
            txn->addIncomingRWEdge(version->writerTransactionId);
            writerIt->second->addOutgoingRWEdge(txn->id);
        }
    }
    
    std::cout << "x" << variableId << ": " << version->value << std::endl;
}

void TransactionManager::readReplicated(std::shared_ptr<Transaction> txn, int variableId) {
    std::set<int> validSites = computeValidSnapshotSites(txn, variableId);
    
    if (validSites.empty()) {
        std::set<int> allSites = getVariableSites(variableId);
        std::set<int> downSites;
        
        for (int site : allSites) {
            if (!siteStates[site].isUp) {
                downSites.insert(site);
            }
        }
        
        if (!downSites.empty()) {
            std::cout << "Transaction " << txn->id << " waits (no valid site)" << std::endl;
            txn->setWaiting(variableId, downSites);
        } else {
            abort(txn, "No valid snapshot for x" + std::to_string(variableId));
        }
        return;
    }
    
    int chosenSite = *validSites.begin();
    auto version = dataManagers[chosenSite]->readVariable(variableId, txn->startTime);
    
    if (!version) {
        std::cout << "Error: No version for x" << variableId << std::endl;
        return;
    }
    
    txn->addRead(variableId, chosenSite, version->value,
                 version->commitTimestamp, version->writerTransactionId);
    
    // ✅ FIX: Track first access time for this site
    if (txn->firstAccessTimePerSite.find(chosenSite) == txn->firstAccessTimePerSite.end()) {
        txn->firstAccessTimePerSite[chosenSite] = currentTimestamp;
    }
    
    // Track RW edge
    if (version->writerTransactionId != "INITIAL" && 
        version->writerTransactionId != txn->id) {
        auto writerIt = transactions.find(version->writerTransactionId);
        if (writerIt != transactions.end()) {
            txn->addIncomingRWEdge(version->writerTransactionId);
            writerIt->second->addOutgoingRWEdge(txn->id);
        }
    }
    
    std::cout << "x" << variableId << ": " << version->value << std::endl;
}

// ============================================================================
// WRITE OPERATIONS
// ============================================================================

void TransactionManager::write(const std::string& transactionId, int variableId, int value) {
    currentTimestamp++;
    
    auto it = transactions.find(transactionId);
    if (it == transactions.end()) {
        std::cout << "Error: Transaction " << transactionId << " not found" << std::endl;
        return;
    }
    
    auto txn = it->second;
    txn->addWrite(variableId, value);
    
    std::set<int> sites = getVariableSites(variableId);
    std::vector<int> sitesWritten;
    
    for (int site : sites) {
        if (siteStates[site].isUp) {
            dataManagers[site]->writeVariable(variableId, value, transactionId);
            txn->writeSites.insert(site);
            txn->writeSet.at(variableId).addSite(site);
            
            if (txn->firstAccessTimePerSite.find(site) == txn->firstAccessTimePerSite.end()) {
                txn->firstAccessTimePerSite[site] = currentTimestamp;
            }
            
            sitesWritten.push_back(site);
        }
    }
    
    std::cout << "W(" << transactionId << ", x" << variableId << ", " << value << ") -> sites:";
    for (int s : sitesWritten) std::cout << " " << s;
    std::cout << std::endl;
}

// ============================================================================
// VALIDATION METHODS
// ============================================================================

std::set<int> TransactionManager::computeValidSnapshotSites(
    std::shared_ptr<Transaction> txn, int variableId) {
    
    std::set<int> validSites;
    std::set<int> allSites = getVariableSites(variableId);
    
    for (int siteId : allSites) {
        if (!siteStates[siteId].isUp) continue;
        
        if (isOddVariable(variableId)) {
            validSites.insert(siteId);
            continue;
        }
        
        if (!dataManagers[siteId]->isReplicaReadable(variableId)) continue;
        
        auto version = dataManagers[siteId]->readVariable(variableId, txn->startTime);
        if (!version) continue;
        
        int commitTime = version->commitTimestamp;
        if (wasSiteUpContinuously(siteId, commitTime, txn->startTime)) {
            validSites.insert(siteId);
        }
    }
    
    return validSites;
}

bool TransactionManager::wasSiteUpContinuously(int siteId, int fromTime, int toTime) {
    return siteStates[siteId].wasUpContinuously(fromTime, toTime);
}

bool TransactionManager::violatesFailureRule(std::shared_ptr<Transaction> txn) {
    for (int siteId : txn->writeSites) {
        int firstAccess = txn->firstAccessTimePerSite[siteId];
        const auto& failureHistory = siteStates[siteId].failureHistory;
        
        for (const auto& interval : failureHistory) {
            if (interval.failTime >= firstAccess && 
                interval.failTime < currentTimestamp) {
                return true;
            }
        }
    }
    
    for (int siteId : txn->criticalReadSites) {
        const auto& failureHistory = siteStates[siteId].failureHistory;
        for (const auto& interval : failureHistory) {
            if (interval.failTime >= txn->startTime && 
                interval.failTime < currentTimestamp) {
                return true;
            }
        }
    }
    
    return false;
}

bool TransactionManager::violatesFirstCommitterWins(std::shared_ptr<Transaction> txn) {
    for (const auto& [variableId, writeInfo] : txn->writeSet) {
        if (variableCommitHistory.find(variableId) == variableCommitHistory.end()) {
            continue;
        }
        
        for (const auto& [committedTxnId, commitTime] : variableCommitHistory[variableId]) {
            if (commitTime > txn->startTime && commitTime < currentTimestamp) {
                return true;
            }
        }
    }
    
    return false;
}

bool TransactionManager::violatesReadWriteCycle(std::shared_ptr<Transaction> txn) {
    std::set<std::string> visited;
    if (hasRWCyclePath(txn->id, txn->id, 0, visited)) {
        return true;
    }
    
    // Check WW edges closing RW cycles
    for (const auto& [variableId, writeInfo] : txn->writeSet) {
        if (variableCommitHistory.find(variableId) == variableCommitHistory.end()) {
            continue;
        }
        
        for (const auto& [committedTxnId, commitTime] : variableCommitHistory[variableId]) {
            if (commitTime <= txn->startTime) continue;
            if (hasPathViaRW(txn->id, committedTxnId)) {
                return true;
            }
        }
    }
    
    return false;
}

bool TransactionManager::hasRWCyclePath(const std::string& fromTxnId, 
                                       const std::string& targetTxnId,
                                       int edgeCount, 
                                       std::set<std::string>& visited) {
    
    if (fromTxnId == targetTxnId && edgeCount >= 2) return true;
    if (visited.find(fromTxnId) != visited.end()) return false;
    visited.insert(fromTxnId);
    
    auto it = transactions.find(fromTxnId);
    if (it == transactions.end()) {
        for (const auto& committedTxn : committedTransactions) {
            if (committedTxn->id == fromTxnId) {
                for (const std::string& nextTxnId : committedTxn->outgoingReadWriteConflicts) {
                    if (hasRWCyclePath(nextTxnId, targetTxnId, edgeCount + 1, visited)) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }
    
    auto txn = it->second;
    for (const std::string& nextTxnId : txn->outgoingReadWriteConflicts) {
        if (hasRWCyclePath(nextTxnId, targetTxnId, edgeCount + 1, visited)) {
            return true;
        }
    }
    
    return false;
}

bool TransactionManager::hasPathViaRW(const std::string& fromTxnId, 
                                     const std::string& toTxnId) {
    std::queue<std::string> q;
    std::set<std::string> visited;
    
    q.push(fromTxnId);
    visited.insert(fromTxnId);
    
    while (!q.empty()) {
        std::string current = q.front();
        q.pop();
        
        if (current == toTxnId) return true;
        
        auto it = transactions.find(current);
        if (it == transactions.end()) {
            for (const auto& committedTxn : committedTransactions) {
                if (committedTxn->id == current) {
                    for (const std::string& next : committedTxn->outgoingReadWriteConflicts) {
                        if (visited.find(next) == visited.end()) {
                            visited.insert(next);
                            q.push(next);
                        }
                    }
                    break;
                }
            }
        } else {
            for (const std::string& next : it->second->outgoingReadWriteConflicts) {
                if (visited.find(next) == visited.end()) {
                    visited.insert(next);
                    q.push(next);
                }
            }
        }
    }
    
    return false;
}

// ============================================================================
// COMMIT/ABORT
// ============================================================================

void TransactionManager::end(const std::string& transactionId) {
    currentTimestamp++;
    
    auto it = transactions.find(transactionId);
    if (it == transactions.end()) {
        std::cout << "Error: Transaction " << transactionId << " not found" << std::endl;
        return;
    }
    
    auto txn = it->second;
    
    if (violatesFailureRule(txn)) {
        abort(txn, "Site failure");
        return;
    }
    
    if (violatesFirstCommitterWins(txn)) {
        abort(txn, "First-committer-wins");
        return;
    }
    
    if (violatesReadWriteCycle(txn)) {
        abort(txn, "RW-cycle");
        return;
    }
    
    commit(txn);
}

void TransactionManager::commit(std::shared_ptr<Transaction> txn) {
    txn->commitTime = currentTimestamp;
    txn->status = TransactionStatus::COMMITTED;
    
    for (int siteId : txn->writeSites) {
        if (siteStates[siteId].isUp) {
            dataManagers[siteId]->commitWrites(txn->id, currentTimestamp);
        }
    }
    
    for (const auto& [variableId, writeInfo] : txn->writeSet) {
        variableCommitHistory[variableId].push_back({txn->id, currentTimestamp});
    }
    
    committedTransactions.push_back(txn);
    transactions.erase(txn->id);
    
    std::cout << txn->id << " commits" << std::endl;
}

void TransactionManager::abort(std::shared_ptr<Transaction> txn, const std::string& reason) {
    txn->status = TransactionStatus::ABORTED;
    
    for (int siteId : txn->writeSites) {
        dataManagers[siteId]->abortWrites(txn->id);
    }
    
    transactions.erase(txn->id);
    
    std::cout << txn->id << " aborts (" << reason << ")" << std::endl;
}

// ============================================================================
// SITE MANAGEMENT
// ============================================================================

void TransactionManager::fail(int siteId) {
    currentTimestamp++;
    std::cout << "Site " << siteId << " fails" << std::endl;
    siteStates[siteId].fail(currentTimestamp);
    dataManagers[siteId]->onFailure();
}

void TransactionManager::recover(int siteId) {
    currentTimestamp++;
    std::cout << "Site " << siteId << " recovers" << std::endl;
    siteStates[siteId].recover(currentTimestamp);
    dataManagers[siteId]->onRecovery(currentTimestamp);
    retryWaitingTransactions(siteId);
}

void TransactionManager::retryWaitingTransactions(int recoveredSiteId) {
    std::vector<std::string> toRetry;
    
    for (auto& [txnId, txn] : transactions) {
        if (txn->status == TransactionStatus::WAITING) {
            if (txn->waitInfo.candidateSites.find(recoveredSiteId) != 
                txn->waitInfo.candidateSites.end()) {
                
                int variableId = txn->waitInfo.variableId;
                
                if (isOddVariable(variableId)) {
                    toRetry.push_back(txnId);
                } else {
                    std::set<int> validSites = computeValidSnapshotSites(txn, variableId);
                    if (!validSites.empty()) {
                        toRetry.push_back(txnId);
                    }
                }
            }
        }
    }
    
    for (const std::string& txnId : toRetry) {
        auto txn = transactions[txnId];
        int variableId = txn->waitInfo.variableId;
        
        std::cout << "Retry: " << txnId << std::endl;
        txn->resumeFromWaiting();
        
        if (isOddVariable(variableId)) {
            readFromHomeSite(txn, variableId);
        } else {
            readReplicated(txn, variableId);
        }
    }
}

void TransactionManager::dump() {
    currentTimestamp++;
    std::cout << "\n=== DUMP ===" << std::endl;
    
    for (int siteId = 1; siteId <= NUM_SITES; siteId++) {
        std::cout << "site " << siteId << " - ";
        auto state = dataManagers[siteId]->getCommittedState();
        
        bool first = true;
        for (int varId = 1; varId <= NUM_VARIABLES; varId++) {
            if (state.find(varId) != state.end()) {
                if (!first) std::cout << ", ";
                std::cout << "x" << varId << ": " << state[varId];
                first = false;
            }
        }
        std::cout << std::endl;
    }
    std::cout << "============\n" << std::endl;
}

} // namespace RepCRec