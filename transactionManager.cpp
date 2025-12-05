// TransactionManager.cpp - Complete Implementation (FIXED)
// Author: RepCRec Team
// Date: December 2024
// Purpose: Full implementation of TransactionManager


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
    
    // Track first access time for this site
    if (txn->firstAccessTimePerSite.find(homeSite) == txn->firstAccessTimePerSite.end()) {
        txn->firstAccessTimePerSite[homeSite] = currentTimestamp;
    }
    
    // NOTE: RW edges are NOT created here anymore.
    // They are created at commit time in createRWEdgesForCommit()
    
    std::cout << "x" << variableId << ": " << version->value << std::endl;
}

void TransactionManager::readReplicated(std::shared_ptr<Transaction> txn, int variableId) {
    std::set<int> validSites = computeValidSnapshotSites(txn, variableId);
    
    if (validSites.empty()) {
        // FIX #3: Check if waiting will ever help
        // We need to find sites that COULD become valid if they recover
        std::set<int> allSites = getVariableSites(variableId);
        std::set<int> potentialSites;  // Sites that might help if they recover
        
        for (int siteId : allSites) {
            if (!siteStates[siteId].isUp) {
                // Check if this site COULD serve a valid snapshot when it recovers
                // A site can help if it was up continuously from the last commit 
                // of variableId before txn->startTime until its failure
                // 
                // We check: was the site up from commitTime to txn->startTime?
                // If the site failed AFTER txn->startTime, it might still be valid when it recovers
                // If the site failed BEFORE txn->startTime, we need to check more carefully
                
                // For simplicity: if the site is down now, it might help when it recovers
                // IF it has a valid version and was up from that version's commit to txn->startTime
                // We can't fully check this without the site being up, so we'll be optimistic
                // and wait for it to recover, then re-check
                
                // Actually, the key insight: if a site failed BEFORE txn->startTime and
                // has not recovered since, then even when it recovers, it won't be valid
                // for txn's snapshot (because it wasn't up continuously to txn->startTime)
                
                // Check if this site was up at txn->startTime
                bool wasUpAtStart = true;
                for (const auto& interval : siteStates[siteId].failureHistory) {
                    // If site was down at txn->startTime
                    if (interval.failTime <= txn->startTime && 
                        (interval.recoverTime == -1 || interval.recoverTime > txn->startTime)) {
                        wasUpAtStart = false;
                        break;
                    }
                }
                
                if (wasUpAtStart) {
                    // Site was up at txn->startTime, so it might have a valid snapshot
                    potentialSites.insert(siteId);
                }
            }
        }
        
        if (!potentialSites.empty()) {
            std::cout << "Transaction " << txn->id << " waits (no valid site)" << std::endl;
            txn->setWaiting(variableId, potentialSites);
        } else {
            // No site can ever become valid for this transaction's snapshot
            abort(txn, "No valid snapshot for x" + std::to_string(variableId));
        }
        return;
    }
    
    int chosenSite = *validSites.begin();
    
    // Read directly from dataStore, bypassing replicaReadEnabled check
    // since computeValidSnapshotSites already verified this site is valid
    auto& versions = dataManagers[chosenSite]->getDataStore()[variableId];
    std::shared_ptr<Version> version = nullptr;
    for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
        if (it->commitTimestamp <= txn->startTime) {
            version = std::make_shared<Version>(*it);
            break;
        }
    }
    
    if (!version) {
        std::cout << "Error: No version for x" << variableId << std::endl;
        return;
    }
    
    txn->addRead(variableId, chosenSite, version->value,
                 version->commitTimestamp, version->writerTransactionId);
    
    // Track first access time for this site
    if (txn->firstAccessTimePerSite.find(chosenSite) == txn->firstAccessTimePerSite.end()) {
        txn->firstAccessTimePerSite[chosenSite] = currentTimestamp;
    }
    
    // NOTE: RW edges are NOT created here anymore.
    // They are created at commit time in createRWEdgesForCommit()
    
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
        
        // For replicated variables, we need to check if this site can serve
        // a valid snapshot for this transaction.
        
        // Get the version that would be visible at txn's snapshot time
        // Note: We temporarily bypass the replicaReadEnabled check here
        // because we need to check if the site was up continuously
        auto& versions = dataManagers[siteId]->getDataStore()[variableId];
        std::shared_ptr<Version> version = nullptr;
        for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
            if (it->commitTimestamp <= txn->startTime) {
                version = std::make_shared<Version>(*it);
                break;
            }
        }
        
        if (!version) continue;
        
        int commitTime = version->commitTimestamp;
        
        // Check if site was up continuously from the commit to txn's start time
        if (wasSiteUpContinuously(siteId, commitTime, txn->startTime)) {
            // This site has a valid snapshot for this transaction.
            // The replicaReadEnabled check is only meant to prevent reading
            // data that might be stale (missing updates that happened while down).
            // But if the transaction started before the site failed, and the site
            // was up continuously from the commit to the transaction's start,
            // then the data is valid for this transaction's snapshot.
            validSites.insert(siteId);
        }
    }
    
    return validSites;
}

bool TransactionManager::wasSiteUpContinuously(int siteId, int fromTime, int toTime) {
    return siteStates[siteId].wasUpContinuously(fromTime, toTime);
}

// FIX #1: Use firstAccessTimePerSite for criticalReadSites instead of startTime
bool TransactionManager::violatesFailureRule(std::shared_ptr<Transaction> txn) {
    // Check write sites
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
    
    // FIX #1: Check critical read sites using firstAccessTimePerSite
    for (int siteId : txn->criticalReadSites) {
        // Get the time when we first accessed this site
        auto accessIt = txn->firstAccessTimePerSite.find(siteId);
        if (accessIt == txn->firstAccessTimePerSite.end()) {
            continue;  // Should not happen, but be safe
        }
        int firstAccess = accessIt->second;
        
        const auto& failureHistory = siteStates[siteId].failureHistory;
        for (const auto& interval : failureHistory) {
            // Site failed AFTER we accessed it
            if (interval.failTime >= firstAccess && 
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

// FIX #2: Completely rewritten RW cycle detection
// RW anti-dependency: T1 reads x, T2 writes x (and commits after T1 started)
// Edge goes FROM reader TO writer: T1 -> T2
bool TransactionManager::violatesReadWriteCycle(std::shared_ptr<Transaction> txn) {
    // First, create RW edges for this committing transaction
    // For each variable this txn wrote, find concurrent txns that read it
    createRWEdgesForCommit(txn);
    
    // Now check for cycles: is there a path from txn back to txn via RW edges?
    // We need at least 2 consecutive RW edges for a dangerous structure
    std::set<std::string> visited;
    if (hasRWCyclePath(txn->id, txn->id, 0, visited)) {
        return true;
    }
    
    // Also check if this txn's writes create a WW conflict that closes an RW path
    // If txn writes x, and some committed txn T' also wrote x,
    // there's a WW edge between them. Combined with RW edges, this could form a cycle.
    //
    // The cycle structure we're looking for:
    // T' --WW--> txn --RW--> ... --RW--> T' (or some chain back to T')
    // OR
    // txn --WW--> T' --RW--> ... --RW--> txn
    //
    // Since WW edge direction is determined by commit order:
    // - If T' committed before txn: WW edge T' -> txn
    //   Cycle: T' -> txn -> ... -> T' (need RW path from txn to T')
    // - If T' committed after txn started (but before now): WW edge txn -> T'
    //   Cycle: txn -> T' -> ... -> txn (need RW path from T' to txn)
    
    for (const auto& [variableId, writeInfo] : txn->writeSet) {
        if (variableCommitHistory.find(variableId) == variableCommitHistory.end()) {
            continue;
        }
        
        for (const auto& [committedTxnId, commitTime] : variableCommitHistory[variableId]) {
            // WW edge exists: T' committed x, txn writes x
            // Check for RW path from txn to T' (this closes cycle T' -> txn -> ... -> T')
            if (hasPathViaRW(txn->id, committedTxnId)) {
                return true;
            }
            
            // Also check reverse: RW path from T' to txn (closes cycle txn -> T' -> ... -> txn)
            // This is needed when txn commits after T', but T' has RW path to txn
            if (hasPathViaRW(committedTxnId, txn->id)) {
                return true;
            }
        }
    }
    
    return false;
}

// FIX #2: Create RW edges when a transaction commits
// For each variable this txn wrote, add RW edges FROM all concurrent readers TO this txn
void TransactionManager::createRWEdgesForCommit(std::shared_ptr<Transaction> committingTxn) {
    for (const auto& [variableId, writeInfo] : committingTxn->writeSet) {
        // Find all transactions (active and committed) that:
        // 1. Started before this txn commits (they're concurrent)
        // 2. Read this variable
        // 3. Read a version that was committed before this txn's write
        
        // Check active transactions
        for (auto& [txnId, txn] : transactions) {
            if (txnId == committingTxn->id) continue;
            if (txn->startTime >= currentTimestamp) continue;  // Started after us
            
            // Did this txn read the variable we're writing?
            auto readIt = txn->readSet.find(variableId);
            if (readIt != txn->readSet.end()) {
                // This txn read the variable, and we're writing it
                // RW edge from reader to writer: txn -> committingTxn
                txn->addOutgoingRWEdge(committingTxn->id);
                committingTxn->addIncomingRWEdge(txn->id);
            }
        }
        
        // Check already committed transactions
        for (auto& txn : committedTransactions) {
            if (txn->id == committingTxn->id) continue;
            // Only consider txns that were concurrent with committingTxn
            // (started before committingTxn commits)
            if (txn->startTime >= currentTimestamp) continue;
            
            // Did this txn read the variable we're writing?
            auto readIt = txn->readSet.find(variableId);
            if (readIt != txn->readSet.end()) {
                // Check if the read version was from before committingTxn's start
                // (meaning committingTxn's write would have been a newer version)
                if (readIt->second.versionTimestamp < currentTimestamp) {
                    // RW edge from reader to writer
                    txn->addOutgoingRWEdge(committingTxn->id);
                    committingTxn->addIncomingRWEdge(txn->id);
                }
            }
        }
    }
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
        
        // FIX #4: Call the read functions which will print the value
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