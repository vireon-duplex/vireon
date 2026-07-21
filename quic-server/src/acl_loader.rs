//! Operator-facing ACL rules file loader.
//!
//! Reads a plain-text rules file and builds an `AclTable` via
//! `AclTableBuilder`. The format is intentionally minimal — one rule
//! per line, fields separated by whitespace — to keep the server free
//! of a TOML/JSON parser dependency and to match the existing
//! line-based configuration style used elsewhere in `quic-server`.
//!
//! # File format
//!
//! ```text
//! # action  pattern            [principal=N]  [actions=A,B,...]
//! allow     tenant.42.*        principal=42   actions=publish,subscribe
//! deny      system.*
//! allow     *.*
//! ```
//!
//! Rules:
//! - Blank lines and lines starting with `#` are ignored.
//! - First token: `allow` or `deny` (case-insensitive).
//! - Second token: the topic pattern (e.g. `tenant.42.*`, `system.*`).
//! - Optional `principal=N` — `N` is a `u64`. Omit for "any principal".
//! - Optional `actions=A,B,...` — comma-separated action names
//!   (`publish`, `subscribe`, `topic_create`, `topic_delete`).
//!   Omit for "all actions".
//! - Rules are evaluated in insertion order; first match wins.
//!
//! # Errors
//!
//! [`load`] returns an error only if the file cannot be read or
//! contains a structurally malformed line (wrong action keyword,
//! unparseable principal id, unknown action name). Individual rule
//! pattern errors are logged via `tracing::warn!` and skipped — this
//! matches `AclTableBuilder`'s own behaviour.

use std::path::Path;

use anyhow::{Context, anyhow};
use topic_acl::{AclTable, AclTableBuilder, Action, ActionSet, PrincipalId};

/// Parses the rules file at `path` and returns a built `AclTable`.
///
/// On success every well-formed rule has been pushed; malformed rules
/// are logged and skipped (matching `AclTableBuilder` semantics).
///
/// # Errors
///
/// Returns an error if the file cannot be read or contains a
/// structurally malformed line.
pub fn load(path: &Path) -> anyhow::Result<AclTable> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read ACL rules file: {}", path.display()))?;
    parse(&raw).with_context(|| format!("failed to parse ACL rules file: {}", path.display()))
}

/// Parses a rules string and returns a built `AclTable`.
///
/// Visible to tests so they don't need a tempfile.
pub fn parse(src: &str) -> anyhow::Result<AclTable> {
    let mut builder = AclTableBuilder::new();
    for (lineno, raw_line) in src.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let rule = parse_line(line).map_err(|e| anyhow!("line {}: {e}", lineno + 1))?;
        builder = match rule.action {
            RuleAction::Allow => builder.allow(&rule.pattern, rule.principal, rule.actions),
            RuleAction::Deny => builder.deny(&rule.pattern, rule.principal, rule.actions),
        };
    }
    builder.build().map_err(|e| anyhow!("build failed: {e:?}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuleAction {
    Allow,
    Deny,
}

struct ParsedRule {
    action: RuleAction,
    pattern: String,
    principal: Option<PrincipalId>,
    actions: ActionSet,
}

/// Parses one non-comment, non-blank line into a `ParsedRule`.
fn parse_line(line: &str) -> anyhow::Result<ParsedRule> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err(anyhow!("expected at least `action pattern` — got `{line}`"));
    }
    let action = match tokens[0].to_ascii_lowercase().as_str() {
        "allow" => RuleAction::Allow,
        "deny" => RuleAction::Deny,
        other => return Err(anyhow!("unknown action `{other}` (expected allow|deny)")),
    };
    let pattern = tokens[1].to_string();

    let mut principal: Option<PrincipalId> = None;
    let mut actions = ActionSet::ALL;
    for tok in &tokens[2..] {
        if let Some(value) = tok.strip_prefix("principal=") {
            let id: u64 = value
                .parse()
                .map_err(|e| anyhow!("principal= value `{value}` not a u64: {e}"))?;
            principal = Some(PrincipalId::new(id));
        } else if let Some(value) = tok.strip_prefix("actions=") {
            actions = parse_actions(value)?;
        } else {
            return Err(anyhow!(
                "unknown token `{tok}` (expected principal=N or actions=...)"
            ));
        }
    }

    Ok(ParsedRule {
        action,
        pattern,
        principal,
        actions,
    })
}

/// Parses a comma-separated action list (`publish,subscribe`).
fn parse_actions(s: &str) -> anyhow::Result<ActionSet> {
    let mut set = ActionSet::NONE;
    for name in s.split(',') {
        let a = match name.trim() {
            "publish" => Action::Publish,
            "subscribe" => Action::Subscribe,
            "topic_create" => Action::TopicCreate,
            "topic_delete" => Action::TopicDelete,
            "" => continue,
            other => return Err(anyhow!("unknown action name `{other}`")),
        };
        set.add(a);
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_produces_empty_table() {
        let t = parse("").unwrap();
        assert_eq!(t.rule_count(), 0);
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let src = "# comment\n\n  \n# another\n";
        let t = parse(src).unwrap();
        assert_eq!(t.rule_count(), 0);
    }

    #[test]
    fn simple_allow_rule_without_principal_or_actions() {
        let t = parse("allow *.*").unwrap();
        assert_eq!(t.rule_count(), 1);
    }

    #[test]
    fn deny_rule_with_principal_bound() {
        let t = parse("deny system.* principal=99").unwrap();
        assert_eq!(t.rule_count(), 1);
    }

    #[test]
    fn allow_rule_with_specific_actions() {
        let t = parse("allow tenant.42.* principal=42 actions=publish,subscribe").unwrap();
        assert_eq!(t.rule_count(), 1);
    }

    #[test]
    fn multiple_rules_preserve_order() {
        let src = "allow private.* principal=1 actions=publish\n\
                   deny  system.*\n\
                   allow *.*";
        let t = parse(src).unwrap();
        assert_eq!(t.rule_count(), 3);
    }

    #[test]
    fn unknown_action_keyword_errors() {
        let err = parse("maybe *.*").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown action"), "got: {msg}");
    }

    #[test]
    fn unknown_action_name_errors() {
        let err = parse("allow x.y actions=frobnicate").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown action name"), "got: {msg}");
    }

    #[test]
    fn line_number_in_error_message() {
        let src = "# header comment\n\nallow *.*\nmaybe system.*\n";
        let err = parse(src).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("line 4"),
            "expected line number in error, got: {msg}"
        );
    }

    #[test]
    fn case_insensitive_action_keyword() {
        let t = parse("ALLOW *.*").unwrap();
        assert_eq!(t.rule_count(), 1);
    }

    #[test]
    fn parsed_table_actually_deny_works() {
        // Round-trip through parse + an actual ACL check.
        let t = parse(
            "deny  system.*\n\
             allow *.*",
        )
        .unwrap();
        // system.logs → first rule matches → Deny.
        assert!(matches!(
            t.check(PrincipalId::new(1), Action::Publish, "system.logs"),
            topic_acl::AclDecision::Deny { .. }
        ));
        // tenant.foo → second rule matches → Allow.
        assert!(
            t.check(PrincipalId::new(1), Action::Publish, "tenant.foo")
                .is_allow()
        );
    }
}
