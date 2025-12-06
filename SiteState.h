// SiteState.h
// Author: Aishwarya and Archita
// Date: December 2025
// Purpose: Track site availability and failure history
// Side effects: Updates failure history on state changes

#ifndef SITESTATE_H
#define SITESTATE_H

#include <vector>

namespace RepCRec {

struct FailureInterval {
    int failTime;
    int recoverTime;  // -1 if still down
    
    FailureInterval(int fTime) : failTime(fTime), recoverTime(-1) {}
};

class SiteState {
public:
    int siteId;
    bool isUp;
    int lastRecoveryTime;
    std::vector<FailureInterval> failureHistory;
    
    SiteState(int id) : siteId(id), isUp(true), lastRecoveryTime(0) {}
    
    // Default constructor needed for map
    SiteState() : siteId(0), isUp(true), lastRecoveryTime(0) {}
    
    // Record a failure
    void fail(int currentTime) {
        isUp = false;
        failureHistory.push_back(FailureInterval(currentTime));
    }
    
    // Record a recovery
    void recover(int currentTime) {
        isUp = true;
        lastRecoveryTime = currentTime;
        
        // Complete the last failure interval
        if (!failureHistory.empty() && failureHistory.back().recoverTime == -1) {
            failureHistory.back().recoverTime = currentTime;
        }
    }
    
    // Check if site was up continuously during a time interval
    bool wasUpContinuously(int fromTime, int toTime) const {
        for (const auto& interval : failureHistory) {
            // Check if this failure overlaps with [fromTime, toTime]
            if (interval.failTime <= toTime && 
                (interval.recoverTime == -1 || interval.recoverTime > fromTime)) {
                return false;  // Site was down during the critical period
            }
        }
        return true;
    }
};

} // namespace RepCRec

#endif // SITESTATE_H