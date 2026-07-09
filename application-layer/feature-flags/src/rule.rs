//! Re-export of rule evaluation types from eval module.
//!
//! This module exists for backwards compatibility and to provide
//! a logical place for rule-related extensions in future phases.

pub use crate::eval::{EvaluationContext, FlagRule, RuleCondition};
