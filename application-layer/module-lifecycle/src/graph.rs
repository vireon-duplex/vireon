//! [`DependencyGraph`] — dependency resolution with cycle detection.
//!
//! See `DESIGN.md` Part 9 (dependency graph).
//!
//! # Role
//!
//! `DependencyGraph` maintains the module dependency structure and
//! provides topological sorting and cycle detection.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::ModuleId;
use crate::config::LifecycleConfig;
use crate::error::ModuleLifecycleError;

/// Dependency graph for module lifecycle.
///
/// Tracks module dependencies and provides topological ordering.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct DependencyGraph {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: LifecycleConfig,
    /// Number of nodes in the graph.
    #[allow(dead_code)]
    node_count: u32,
}

impl DependencyGraph {
    /// Construct a new empty graph.
    #[must_use]
    pub fn new(cfg: LifecycleConfig) -> Self {
        Self { cfg, node_count: 0 }
    }

    /// Add a dependency edge from one module to another.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn add_edge(&mut self, _from: ModuleId, _to: ModuleId) -> Result<(), ModuleLifecycleError> {
        // TODO(phase-1): add edge + cycle check.
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Compute a topological sort of modules.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn topo_sort(&self) -> Result<Vec<ModuleId>, ModuleLifecycleError> {
        // TODO(phase-1): Kahn's algorithm or DFS-based sort.
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Check if the graph contains a cycle.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn has_cycle(&self) -> Result<bool, ModuleLifecycleError> {
        // TODO(phase-1): DFS-based cycle detection.
        Err(ModuleLifecycleError::ShuttingDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_graph() -> DependencyGraph {
        let cfg = Preset::Eager.to_config();
        DependencyGraph::new(cfg)
    }

    #[test]
    fn graph_constructs_from_config() {
        let graph = make_graph();
        assert_eq!(graph.node_count, 0);
    }

    #[test]
    fn add_edge_phase0_stub() {
        let mut graph = make_graph();
        let from = ModuleId::new(1);
        let to = ModuleId::new(2);
        assert!(graph.add_edge(from, to).is_err());
    }

    #[test]
    fn topo_sort_phase0_stub() {
        let graph = make_graph();
        assert!(graph.topo_sort().is_err());
    }

    #[test]
    fn has_cycle_phase0_stub() {
        let graph = make_graph();
        assert!(graph.has_cycle().is_err());
    }
}
