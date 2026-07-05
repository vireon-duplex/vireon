//! [`ConnectionState`] — lifecycle state machine (Layer 5).
//!
//! ```text
//!                 create()
//!     ┌──────┐ ─────────────► ┌──────┐
//!     │ (nil) │                │ Init │
//!     └──────┘                └──┬───┘
//!                                │ activate()
//!                           ┌────▼─────┐
//!                     ┌─────│  Active  │◄────────┐
//!                     │     └────┬─────┘         │
//!               pressurize()     │ relieve()     │
//!                     │     ┌────▼──────────┐    │
//!                     ├────►│Backpressured  │────┘
//!                     │     └────┬──────────┘
//!                     │          │ begin_close()
//!              migrate()    ┌────▼─────┐
//!                     ├────►│ Migrating │
//!                     │     └────┬─────┘
//!                     │          │ complete_migration_out
//!                begin_close()   │ (on source)
//!                     │     ┌────▼─────┐
//!                     ├────►│ Closing  │
//!                           └────┬─────┘
//!                                │ drain
//!                           ┌────▼─────┐
//!                           │  Closed  │
//!                           └────┬─────┘
//!                                │ drain_closed / recycle
//!                           ┌────▼─────┐
//!                           │  (nil)   │
//!                           └──────────┘
//! ```

/// Per-connection lifecycle state.
///
/// Stored inline in each slab entry. The state machine is enforced by
/// [`ConnectionShard`](crate::shard::ConnectionShard) transition methods;
/// illegal transitions return [`ShardError::IllegalTransition`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ConnectionState {
    /// Slot allocated but connection not yet activated.
    ///
    /// Entered via `create()`. The connection is known to the shard but
    /// does not accept traffic until `activate()` is called.
    #[default]
    Init = 0,

    /// Connection is live and accepting traffic.
    ///
    /// Entered via `activate()`. This is the steady-state for a healthy
    /// connection.
    Active = 1,

    /// Connection is under backpressure (soft or hard).
    ///
    /// Entered when the shard detects resource pressure. The connection
    /// is still live but should throttle. Returns to `Active` when
    /// pressure is relieved.
    Backpressured = 2,

    /// Ownership is being transferred to another shard.
    ///
    /// Entered via `prepare_migration()`. During this state the
    /// connection is frozen: no new traffic is accepted. Resolved by
    /// `complete_migration_out()` (source removes) or rollback.
    Migrating = 3,

    /// Graceful close initiated; draining in-flight work.
    ///
    /// Entered via `begin_close()`. No new traffic is accepted; existing
    /// in-flight operations are allowed to complete.
    Closing = 4,

    /// Finalized; slot is pending recycle.
    ///
    /// The slot can be reclaimed by `drain_closed()`. Once recycled,
    /// the generation is bumped and the slot returns to the free list.
    Closed = 5,
}

impl ConnectionState {
    /// Whether the connection is live (occupies a slot and may have
    /// traffic or state). `Init`, `Active`, `Backpressured` are live.
    #[inline]
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(self, Self::Init | Self::Active | Self::Backpressured)
    }

    /// Whether the connection is fully terminated and the slot can be
    /// recycled.
    #[inline]
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Whether the connection can accept new packets.
    ///
    /// `Active` and `Backpressured` accept packets (backpressured
    /// connections still receive — they just send slower).
    #[inline]
    #[must_use]
    pub const fn can_accept(self) -> bool {
        matches!(self, Self::Active | Self::Backpressured)
    }

    /// Whether the connection can be migrated to another shard.
    #[inline]
    #[must_use]
    pub const fn can_migrate(self) -> bool {
        matches!(self, Self::Active | Self::Backpressured)
    }

    /// Whether the connection can begin a graceful close.
    #[inline]
    #[must_use]
    pub const fn can_close(self) -> bool {
        matches!(self, Self::Init | Self::Active | Self::Backpressured)
    }

    /// Whether the connection is in a transient state (not steady).
    #[inline]
    #[must_use]
    pub const fn is_transient(self) -> bool {
        matches!(self, Self::Migrating | Self::Closing | Self::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_init() {
        assert_eq!(ConnectionState::default(), ConnectionState::Init);
    }

    #[test]
    fn is_live_matrix() {
        assert!(ConnectionState::Init.is_live());
        assert!(ConnectionState::Active.is_live());
        assert!(ConnectionState::Backpressured.is_live());
        assert!(!ConnectionState::Migrating.is_live());
        assert!(!ConnectionState::Closing.is_live());
        assert!(!ConnectionState::Closed.is_live());
    }

    #[test]
    fn is_terminal_matrix() {
        assert!(!ConnectionState::Init.is_terminal());
        assert!(!ConnectionState::Active.is_terminal());
        assert!(ConnectionState::Closed.is_terminal());
    }

    #[test]
    fn can_accept_matrix() {
        assert!(!ConnectionState::Init.can_accept());
        assert!(ConnectionState::Active.can_accept());
        assert!(ConnectionState::Backpressured.can_accept());
        assert!(!ConnectionState::Migrating.can_accept());
        assert!(!ConnectionState::Closing.can_accept());
        assert!(!ConnectionState::Closed.can_accept());
    }

    #[test]
    fn can_migrate_matrix() {
        assert!(!ConnectionState::Init.can_migrate());
        assert!(ConnectionState::Active.can_migrate());
        assert!(ConnectionState::Backpressured.can_migrate());
        assert!(!ConnectionState::Migrating.can_migrate());
        assert!(!ConnectionState::Closing.can_migrate());
    }

    #[test]
    fn can_close_matrix() {
        assert!(ConnectionState::Init.can_close());
        assert!(ConnectionState::Active.can_close());
        assert!(ConnectionState::Backpressured.can_close());
        assert!(!ConnectionState::Migrating.can_close());
        assert!(!ConnectionState::Closing.can_close());
        assert!(!ConnectionState::Closed.can_close());
    }

    #[test]
    fn is_transient_matrix() {
        assert!(!ConnectionState::Active.is_transient());
        assert!(ConnectionState::Migrating.is_transient());
        assert!(ConnectionState::Closing.is_transient());
        assert!(ConnectionState::Closed.is_transient());
    }

    #[test]
    fn repr_u8_values() {
        // Ensure discriminants are stable for FFI / packing.
        assert_eq!(ConnectionState::Init as u8, 0);
        assert_eq!(ConnectionState::Active as u8, 1);
        assert_eq!(ConnectionState::Backpressured as u8, 2);
        assert_eq!(ConnectionState::Migrating as u8, 3);
        assert_eq!(ConnectionState::Closing as u8, 4);
        assert_eq!(ConnectionState::Closed as u8, 5);
    }
}
