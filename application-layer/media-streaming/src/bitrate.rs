//! [`BitrateLadder`], [`AdaptiveBitrateConfig`] — adaptive bitrate
//! management for simulcast and ABR.
//!
//! See `DESIGN.md` Part 6 (adaptive bitrate).

use crate::error::MediaError;

// ── BitrateLadder ──────────────────────────────────────────────────

/// A sorted ladder of bitrate layers (layer_id → bitrate_bps).
///
/// Layers are stored in descending bitrate order: highest at index 0,
/// lowest at the last populated slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BitrateLadder {
    /// Array of (layer_id, bitrate_bps) pairs, sorted descending.
    layers: [(u8, u32); 8],
    /// Number of valid entries in the array (0..=8).
    count: u8,
}

impl BitrateLadder {
    /// Maximum number of layers.
    pub const MAX_LAYERS: u8 = 8;

    /// Construct an empty ladder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            layers: [(0, 0); 8],
            count: 0,
        }
    }

    /// Add a layer to the ladder.
    ///
    /// # Errors
    /// Returns [`MediaError::BandwidthExceeded`] if the ladder is full.
    pub fn add_layer(&mut self, layer_id: u8, bitrate_bps: u32) -> Result<(), MediaError> {
        if self.count >= Self::MAX_LAYERS {
            return Err(MediaError::BandwidthExceeded);
        }

        // Insert and sort by bitrate descending.
        self.layers[self.count as usize] = (layer_id, bitrate_bps);
        self.count += 1;

        // Sort in descending order by bitrate.
        self.layers[..self.count as usize].sort_by_key(|(_, bps)| std::cmp::Reverse(*bps));

        Ok(())
    }

    /// Find the best layer for a given available bandwidth.
    ///
    /// Returns the highest-quality layer whose bitrate does not exceed
    /// the available bandwidth. Returns None if all layers exceed the
    /// bandwidth or the ladder is empty.
    #[must_use]
    pub fn best_for_bandwidth(&self, available_bps: u32) -> Option<u8> {
        for &(layer_id, bitrate_bps) in self.layers.iter().take(self.count as usize) {
            if bitrate_bps <= available_bps {
                return Some(layer_id);
            }
        }
        None
    }

    /// Get the highest-quality layer ID.
    ///
    /// Returns None if the ladder is empty.
    #[must_use]
    pub const fn highest(&self) -> Option<u8> {
        if self.count > 0 {
            Some(self.layers[0].0)
        } else {
            None
        }
    }

    /// Get the lowest-quality layer ID.
    ///
    /// Returns None if the ladder is empty.
    #[must_use]
    pub const fn lowest(&self) -> Option<u8> {
        if self.count > 0 {
            Some(self.layers[self.count as usize - 1].0)
        } else {
            None
        }
    }

    /// Number of layers in the ladder.
    #[must_use]
    pub const fn len(&self) -> u8 {
        self.count
    }

    /// Whether the ladder is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the layer at the given index.
    ///
    /// Returns None if the index is out of bounds.
    #[must_use]
    pub const fn get(&self, index: u8) -> Option<(u8, u32)> {
        if index < self.count {
            Some(self.layers[index as usize])
        } else {
            None
        }
    }
}

// ── AdaptiveBitrateConfig ───────────────────────────────────────────

/// Configuration for adaptive bitrate control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdaptiveBitrateConfig {
    /// Minimum bitrate in bps.
    pub min_bps: u32,
    /// Maximum bitrate in bps.
    pub max_bps: u32,
    /// Starting bitrate in bps.
    pub start_bps: u32,
    /// Bandwidth threshold for upgrading (in bps above current).
    pub upgrade_threshold_bps: u32,
    /// Bandwidth threshold for downgrading (in bps below current).
    pub downgrade_threshold_bps: u32,
}

impl AdaptiveBitrateConfig {
    /// Construct a new ABR config.
    ///
    /// # Errors
    /// Returns [`MediaError::InvalidSdp`] if parameters are invalid.
    pub fn new(
        min_bps: u32,
        max_bps: u32,
        start_bps: u32,
        upgrade_threshold_bps: u32,
        downgrade_threshold_bps: u32,
    ) -> Result<Self, MediaError> {
        let cfg = Self {
            min_bps,
            max_bps,
            start_bps,
            upgrade_threshold_bps,
            downgrade_threshold_bps,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns [`MediaError::InvalidSdp`] if parameters are invalid.
    pub fn validate(&self) -> Result<(), MediaError> {
        if self.min_bps == 0 || self.max_bps == 0 || self.start_bps == 0 {
            return Err(MediaError::InvalidSdp(
                "bitrate values must be positive".into(),
            ));
        }
        if self.min_bps >= self.max_bps {
            return Err(MediaError::InvalidSdp(
                "min_bps must be less than max_bps".into(),
            ));
        }
        if self.start_bps < self.min_bps || self.start_bps > self.max_bps {
            return Err(MediaError::InvalidSdp(
                "start_bps must be between min and max".into(),
            ));
        }
        Ok(())
    }

    /// Construct a default config for video conferencing.
    #[must_use]
    pub const fn video_conference() -> Self {
        Self {
            min_bps: 300_000,
            max_bps: 5_000_000,
            start_bps: 1_500_000,
            upgrade_threshold_bps: 500_000,
            downgrade_threshold_bps: 1_000_000,
        }
    }

    /// Construct a default config for screen sharing.
    #[must_use]
    pub const fn screen_share() -> Self {
        Self {
            min_bps: 1_000_000,
            max_bps: 20_000_000,
            start_bps: 5_000_000,
            upgrade_threshold_bps: 2_000_000,
            downgrade_threshold_bps: 3_000_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitrate_ladder_new_is_empty() {
        let ladder = BitrateLadder::new();
        assert!(ladder.is_empty());
        assert_eq!(ladder.len(), 0);
        assert!(ladder.highest().is_none());
        assert!(ladder.lowest().is_none());
    }

    #[test]
    fn bitrate_ladder_add_layer() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 1_000_000).unwrap();
        assert_eq!(ladder.len(), 1);
        assert!(!ladder.is_empty());
        assert_eq!(ladder.highest(), Some(0));
        assert_eq!(ladder.lowest(), Some(0));
    }

    #[test]
    fn bitrate_ladder_sorts_descending() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(2, 500_000).unwrap();
        ladder.add_layer(0, 2_000_000).unwrap();
        ladder.add_layer(1, 1_000_000).unwrap();

        // Should be sorted: layer 0 (2M), layer 1 (1M), layer 2 (500K)
        assert_eq!(ladder.get(0), Some((0, 2_000_000)));
        assert_eq!(ladder.get(1), Some((1, 1_000_000)));
        assert_eq!(ladder.get(2), Some((2, 500_000)));
    }

    #[test]
    fn bitrate_ladder_best_for_bandwidth() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 3_000_000).unwrap();
        ladder.add_layer(1, 1_500_000).unwrap();
        ladder.add_layer(2, 500_000).unwrap();

        // After sorting, layers are: (0, 3M), (1, 1.5M), (2, 500K)

        // Exactly match a layer.
        assert_eq!(ladder.best_for_bandwidth(1_500_000), Some(1));

        // Between layers - pick the highest layer ≤ bandwidth
        // For 2M: layer 1 (1.5M) is the highest ≤ 2M
        assert_eq!(ladder.best_for_bandwidth(2_000_000), Some(1));
        // For 1M: layer 2 (500K) is the highest ≤ 1M
        assert_eq!(ladder.best_for_bandwidth(1_000_000), Some(2));

        // Below all layers.
        assert_eq!(ladder.best_for_bandwidth(400_000), None);

        // Above all layers.
        assert_eq!(ladder.best_for_bandwidth(5_000_000), Some(0));
    }

    #[test]
    fn bitrate_ladder_capacity_limit() {
        let mut ladder = BitrateLadder::new();
        for i in 0..8 {
            ladder.add_layer(i, 100_000 * (i + 1) as u32).unwrap();
        }
        // Adding a 9th layer should fail.
        assert!(matches!(
            ladder.add_layer(8, 100_000),
            Err(MediaError::BandwidthExceeded)
        ));
        assert_eq!(ladder.len(), 8);
    }

    #[test]
    fn bitrate_ladder_highest_lowest() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 3_000_000).unwrap();
        ladder.add_layer(1, 1_000_000).unwrap();
        ladder.add_layer(2, 500_000).unwrap();

        assert_eq!(ladder.highest(), Some(0));
        assert_eq!(ladder.lowest(), Some(2));
    }

    #[test]
    fn bitrate_ladder_get_out_of_bounds() {
        let ladder = BitrateLadder::new();
        assert_eq!(ladder.get(0), None);
        assert_eq!(ladder.get(100), None);

        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 1_000_000).unwrap();
        assert_eq!(ladder.get(1), None);
    }

    #[test]
    fn abr_config_new_validates() {
        let cfg =
            AdaptiveBitrateConfig::new(300_000, 5_000_000, 1_500_000, 500_000, 1_000_000).unwrap();
        assert_eq!(cfg.min_bps, 300_000);
        assert_eq!(cfg.max_bps, 5_000_000);
    }

    #[test]
    fn abr_config_rejects_zero_bitrate() {
        let res = AdaptiveBitrateConfig::new(0, 5_000_000, 1_500_000, 500_000, 1_000_000);
        assert!(res.is_err());

        let res = AdaptiveBitrateConfig::new(300_000, 0, 1_500_000, 500_000, 1_000_000);
        assert!(res.is_err());
    }

    #[test]
    fn abr_config_rejects_min_gte_max() {
        let res = AdaptiveBitrateConfig::new(5_000_000, 3_000_000, 1_500_000, 500_000, 1_000_000);
        assert!(res.is_err());

        let res = AdaptiveBitrateConfig::new(3_000_000, 3_000_000, 1_500_000, 500_000, 1_000_000);
        assert!(res.is_err());
    }

    #[test]
    fn abr_config_rejects_start_out_of_range() {
        let res = AdaptiveBitrateConfig::new(300_000, 5_000_000, 200_000, 500_000, 1_000_000);
        assert!(res.is_err());

        let res = AdaptiveBitrateConfig::new(300_000, 5_000_000, 6_000_000, 500_000, 1_000_000);
        assert!(res.is_err());
    }

    #[test]
    fn abr_config_video_conference() {
        let cfg = AdaptiveBitrateConfig::video_conference();
        assert_eq!(cfg.min_bps, 300_000);
        assert_eq!(cfg.max_bps, 5_000_000);
        assert_eq!(cfg.start_bps, 1_500_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn abr_config_screen_share() {
        let cfg = AdaptiveBitrateConfig::screen_share();
        assert_eq!(cfg.min_bps, 1_000_000);
        assert_eq!(cfg.max_bps, 20_000_000);
        assert_eq!(cfg.start_bps, 5_000_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn abr_config_equality() {
        let a = AdaptiveBitrateConfig::video_conference();
        let b = AdaptiveBitrateConfig::video_conference();
        assert_eq!(a, b);

        let c = AdaptiveBitrateConfig::screen_share();
        assert_ne!(a, c);
    }

    #[test]
    fn abr_config_copy() {
        let a = AdaptiveBitrateConfig::video_conference();
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn abr_config_clone() {
        let a = AdaptiveBitrateConfig::video_conference();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn bitrate_ladder_copy() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 1_000_000).unwrap();
        let ladder2 = ladder;
        assert_eq!(ladder.len(), ladder2.len());
    }

    #[test]
    fn bitrate_ladder_clone() {
        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 1_000_000).unwrap();
        let ladder2 = ladder.clone();
        assert_eq!(ladder.len(), ladder2.len());
    }
}
