/// Transaction state as reported by PostgreSQL ReadyForQuery message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 'I' - Idle, not in a transaction
    Idle,
    /// 'T' - In a transaction block
    InTransaction,
    /// 'E' - In a failed transaction block
    InError,
}

/// Tracks transaction state for a client session
#[derive(Debug)]
pub struct TransactionTracker {
    state: TransactionState,
}

impl TransactionTracker {
    pub fn new() -> Self {
        Self {
            state: TransactionState::Idle,
        }
    }

    /// Update state from ReadyForQuery message status byte
    pub fn update_from_ready_for_query(&mut self, status: u8) {
        self.state = match status {
            b'I' => TransactionState::Idle,
            b'T' => TransactionState::InTransaction,
            b'E' => TransactionState::InError,
            _ => self.state, // Unknown status, keep current
        };
    }

    /// Get current transaction state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Check if currently in a transaction (T or E)
    pub fn is_in_transaction(&self) -> bool {
        matches!(self.state, TransactionState::InTransaction | TransactionState::InError)
    }

    /// Check if transaction just completed (state changed to Idle)
    pub fn is_idle(&self) -> bool {
        self.state == TransactionState::Idle
    }
}

impl Default for TransactionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_idle() {
        let tracker = TransactionTracker::new();
        assert_eq!(tracker.state(), TransactionState::Idle);
    }

    #[test]
    fn test_transition_to_in_transaction() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'T');
        assert_eq!(tracker.state(), TransactionState::InTransaction);
    }

    #[test]
    fn test_transition_to_error() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'E');
        assert_eq!(tracker.state(), TransactionState::InError);
    }

    #[test]
    fn test_transition_back_to_idle() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'T');
        tracker.update_from_ready_for_query(b'I');
        assert_eq!(tracker.state(), TransactionState::Idle);
    }

    #[test]
    fn test_is_in_transaction() {
        let mut tracker = TransactionTracker::new();
        assert!(!tracker.is_in_transaction());
        tracker.update_from_ready_for_query(b'T');
        assert!(tracker.is_in_transaction());
    }
}
