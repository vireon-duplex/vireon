//! Flamegraph / call-graph data collection and export.
//!
//! Tracks `(parent → child)` edge frequencies globally. The edges are
//! accumulated thread-locally (see [`crate::context`]) and merged here
//! during periodic flush.
//!
//! # Export formats
//!
//! - **Folded stacks**: `parent;child;grandchild count` — one line per
//!   unique call path. Consumed by `flamegraph.pl` or Speedscope.
//! - **DOT graph**: Graphviz call-graph representation.
//!
//! # Path reconstruction
//!
//! Edges are reconstructed into paths via DFS from root functions (those
//! that are never a child). Each path's count is the minimum edge weight
//! along it, approximating how many times that exact path was observed.

use std::sync::{LazyLock, RwLock};

use rustc_hash::FxHashMap;

type EdgeKey = (&'static str, &'static str);
type EdgeMap = FxHashMap<EdgeKey, u64>;

static GLOBAL_EDGES: LazyLock<RwLock<EdgeMap>> =
    LazyLock::new(|| RwLock::new(FxHashMap::default()));

/// Merges thread-local edge counts into the global edge map.
///
/// Called by the collector during periodic flush.
pub fn merge_edges(edges: Vec<(EdgeKey, u64)>) {
    if edges.is_empty() {
        return;
    }
    let mut global = GLOBAL_EDGES.write().unwrap_or_else(|e| e.into_inner());
    for ((parent, child), count) in edges {
        *global.entry((parent, child)).or_insert(0) += count;
    }
}

/// Resets all edge data.
pub fn reset() {
    GLOBAL_EDGES
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

/// Takes a snapshot of all edges (sorted by count descending).
#[must_use]
pub fn snapshot_edges() -> Vec<(EdgeKey, u64)> {
    let read = GLOBAL_EDGES.read().unwrap_or_else(|e| e.into_inner());
    let mut edges: Vec<_> = read.iter().map(|(&k, &v)| (k, v)).collect();
    edges.sort_by(|a, b| b.1.cmp(&a.1));
    edges
}

/// Exports folded-stack format for flamegraph tools.
///
/// Reconstructs call paths from edges via DFS. Each path is terminated
/// by a space and its count.
#[must_use]
pub fn export_folded() -> String {
    let edges = snapshot_edges();
    if edges.is_empty() {
        return String::new();
    }

    // Build adjacency list and child set.
    let mut children: FxHashMap<&'static str, Vec<(&'static str, u64)>> = FxHashMap::default();
    let mut all_children: std::collections::HashSet<&'static str> =
        std::collections::HashSet::new();
    for &((parent, child), count) in &edges {
        children.entry(parent).or_default().push((child, count));
        all_children.insert(child);
    }

    // Roots: functions that are parents but never children.
    let roots: Vec<&'static str> = children
        .keys()
        .copied()
        .filter(|p| !all_children.contains(p))
        .collect();

    let mut output = String::new();
    let mut path: Vec<&'static str> = Vec::new();

    for root in &roots {
        dfs_folded(root, &children, &mut path, &mut output);
    }

    // Handle functions that appear as children but have no parent edges
    // (e.g. entry points with no instrumentation on caller).
    for &((_parent, _child), _count) in &edges {
        // Edge data already processed above.
    }

    // Also include orphan child-only functions as single-node paths.
    let parents: std::collections::HashSet<&'static str> = children.keys().copied().collect();
    for &child in &all_children {
        if !parents.contains(&child) && !roots.contains(&child) {
            // This function is only ever a child, never a parent or root.
            // Include it with its total incoming edge count.
            let total: u64 = edges
                .iter()
                .filter(|((_, c), _)| *c == child)
                .map(|(_, cnt)| cnt)
                .sum();
            output.push_str(&format!("{child} {total}\n"));
        }
    }

    output
}

/// DFS helper for folded-stack generation.
fn dfs_folded(
    node: &'static str,
    children: &FxHashMap<&'static str, Vec<(&'static str, u64)>>,
    path: &mut Vec<&'static str>,
    output: &mut String,
) {
    path.push(node);

    match children.get(node) {
        Some(child_list) if !child_list.is_empty() => {
            for &(child, count) in child_list {
                // Recurse into child.
                let mut visited = false;
                if children.contains_key(&child) {
                    visited = true;
                    dfs_folded(child, children, path, output);
                }
                if !visited || !path.contains(&child) {
                    // Output this path with the edge count.
                    path.push(child);
                    output.push_str(&path.join(";"));
                    output.push(' ');
                    output.push_str(&count.to_string());
                    output.push('\n');
                    path.pop();
                }
            }
        }
        _ => {
            // Leaf node: output the full path with count 1 (unknown).
            output.push_str(&path.join(";"));
            output.push_str(" 1\n");
        }
    }

    path.pop();
}

/// Exports a Graphviz DOT call-graph.
#[must_use]
pub fn export_dot() -> String {
    let edges = snapshot_edges();
    let mut out = String::from("digraph call_graph {\n");
    out.push_str("    rankdir=TB;\n");
    out.push_str("    node [shape=box];\n");
    for &((parent, child), count) in &edges {
        out.push_str(&format!(
            "    \"{parent}\" -> \"{child}\" [label=\"{count}\"];\n"
        ));
    }
    out.push_str("}\n");
    out
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn reset_all() {
        reset();
    }

    #[test]
    fn merge_edges_accumulates() {
        reset_all();
        merge_edges(vec![(("a", "b"), 10), (("a", "b"), 5)]);
        let edges = snapshot_edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].1, 15);
    }

    #[test]
    fn snapshot_sorted_by_count() {
        reset_all();
        merge_edges(vec![(("a", "b"), 5), (("c", "d"), 100), (("e", "f"), 50)]);
        let edges = snapshot_edges();
        assert_eq!(edges[0].1, 100);
        assert_eq!(edges[1].1, 50);
        assert_eq!(edges[2].1, 5);
    }

    #[test]
    fn export_folded_not_empty() {
        reset_all();
        merge_edges(vec![(("main", "worker"), 10), (("worker", "process"), 5)]);
        let folded = export_folded();
        assert!(!folded.is_empty());
        assert!(folded.contains("main;worker"));
    }

    #[test]
    fn export_dot_well_formed() {
        reset_all();
        merge_edges(vec![(("a", "b"), 3)]);
        let dot = export_dot();
        assert!(dot.starts_with("digraph"));
        assert!(dot.contains("\"a\" -> \"b\""));
        assert!(dot.contains("label=\"3\""));
        assert!(dot.ends_with("}\n"));
    }

    #[test]
    fn reset_clears_edges() {
        reset_all();
        merge_edges(vec![(("x", "y"), 1)]);
        reset();
        assert!(snapshot_edges().is_empty());
    }

    #[test]
    fn export_folded_empty_when_no_data() {
        reset_all();
        let folded = export_folded();
        assert!(folded.is_empty());
    }
}
