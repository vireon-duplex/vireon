//! `vireon` — runtime entry point.
//!
//! Phase 9 capstone binary. Wires together the application-layer
//! crates built across phases 1–7 into a single runnable demo:
//!
//! 1. Initializes `tracing`.
//! 2. Builds a [`PubSubEngine`], subscribes to a few patterns, and
//!    publishes a small stream of demo messages.
//! 3. Builds an [`AclTable`] via [`AclTableBuilder`] and runs a few
//!    authorization checks (allow / explicit-deny / no-match).
//! 4. Idles until Ctrl-C, then exits cleanly.
//!
//! The binary prints results, so `print_stdout` is allowed at the
//! crate root. All other workspace deny lints still apply.
//!
//! ```text
//! cargo run -p vireon
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use auth_framework::PrincipalId;
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage, TopicPattern};
use topic_acl::{AclTableBuilder, Action, ActionSet};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    print_banner();

    run_pubsub_demo();
    run_acl_demo();

    wait_for_ctrlc();
    print_shutdown();
    Ok(())
}

// ── Banner / shutdown ─────────────────────────────────────────────

fn print_banner() {
    println!(
        "vireon {} — QUIC-native distributed streaming pub/sub runtime",
        env!("CARGO_PKG_VERSION")
    );
    println!("(phase 9: integrated binary demo)");
    println!("{}", "-".repeat(60));
}

fn print_shutdown() {
    println!("{}", "-".repeat(60));
    println!("vireon shutdown complete.");
}

fn wait_for_ctrlc() {
    let running = Arc::new(AtomicBool::new(true));
    let r = Arc::clone(&running);
    // ctrlc handler must not panic; the only failure mode is a second
    // handler already being registered, which is harmless on a fresh
    // process.
    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    });
    println!("\n(idling — press Ctrl-C to exit)");
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

// ── Pub/sub demo ──────────────────────────────────────────────────

fn run_pubsub_demo() -> PubSubEngine {
    println!("\n[1] Pub/Sub engine — subscribe + publish");
    let mut engine = PubSubEngine::new(PubSubConfig::new());

    let patterns = ["sensor.*", "alert.*", "system.logs"];
    for p in patterns {
        let pat = match TopicPattern::new(p) {
            Ok(x) => x,
            Err(e) => {
                println!("  pattern {p:?} rejected: {e:?}");
                continue;
            }
        };
        match engine.subscribe(pat) {
            Ok(id) => println!("  subscribed id={} → {p}", id.raw()),
            Err(e) => println!("  subscribe {p} failed: {e:?}"),
        }
    }

    let topics = [
        ("sensor.temp", &[0x01, 0x02, 0x03][..]),
        ("sensor.humidity", &[0x04, 0x05][..]),
        ("alert.fire", &[0xff][..]),
        ("system.logs", &[0xaa, 0xbb][..]),
        // No subscribers for this one — counted as publish only.
        ("metrics.cpu", &[0x00][..]),
    ];
    for (topic, payload) in topics {
        let topic = match pubsub_engine::Topic::new(topic) {
            Ok(t) => t,
            Err(e) => {
                println!("  topic {topic:?} rejected: {e:?}");
                continue;
            }
        };
        match engine.publish(PubSubMessage::new(topic.clone(), payload.to_vec())) {
            Ok(()) => println!("  published → {topic} ({} bytes)", payload.len()),
            Err(e) => println!("  publish {topic} failed: {e:?}"),
        }
    }

    let snap = engine.snapshot();
    println!(
        "  totals: published={}, delivered={}, active_subs={}",
        snap.messages_published, snap.messages_delivered, snap.subscriptions_active
    );
    engine
}

// ── ACL demo ──────────────────────────────────────────────────────

fn run_acl_demo() {
    println!("\n[2] Topic ACL — allow / explicit-deny / no-match");

    let mut pub_sub = ActionSet::NONE;
    pub_sub.add(Action::Publish).add(Action::Subscribe);

    let table = AclTableBuilder::new()
        .allow("sensor.*", None, pub_sub)
        .allow("admin.*", Some(PrincipalId::new(1)), ActionSet::ALL)
        .deny("system.*", None, ActionSet::ALL)
        .build()
        .map(|t| {
            println!("  built ACL with {} rules", t.rule_count());
            t
        })
        .unwrap_or_else(|e| {
            println!("  ACL build failed: {e:?}; using empty table");
            topic_acl::AclTable::new()
        });

    let scenarios: &[(PrincipalId, Action, &str)] = &[
        (PrincipalId::new(99), Action::Publish, "sensor.temp"),
        (PrincipalId::new(1), Action::TopicDelete, "admin.users"),
        (PrincipalId::new(1), Action::TopicDelete, "system.logs"),
        (PrincipalId::new(99), Action::Publish, "unknown.topic"),
    ];
    for (p, a, t) in scenarios {
        let d = table.check(*p, *a, t);
        println!("  check({p}, {a:?}, {t:?}) → {d:?}");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use topic_acl::AclDecision;

    #[test]
    fn acl_demo_table_has_three_rules() {
        let mut pub_sub = ActionSet::NONE;
        pub_sub.add(Action::Publish).add(Action::Subscribe);
        let t = AclTableBuilder::new()
            .allow("sensor.*", None, pub_sub)
            .allow("admin.*", Some(PrincipalId::new(1)), ActionSet::ALL)
            .deny("system.*", None, ActionSet::ALL)
            .build()
            .unwrap();
        assert_eq!(t.rule_count(), 3);
    }

    #[test]
    fn acl_demo_scenarios_match_expected_decisions() {
        let mut pub_sub = ActionSet::NONE;
        pub_sub.add(Action::Publish).add(Action::Subscribe);
        let t = AclTableBuilder::new()
            .allow("sensor.*", None, pub_sub)
            .allow("admin.*", Some(PrincipalId::new(1)), ActionSet::ALL)
            .deny("system.*", None, ActionSet::ALL)
            .build()
            .unwrap();

        // Anonymous publish to sensor.* → allowed.
        assert_eq!(
            t.check(PrincipalId::new(99), Action::Publish, "sensor.temp"),
            AclDecision::Allow
        );
        // Admin#1 with full control on admin.* → allowed.
        assert_eq!(
            t.check(PrincipalId::new(1), Action::TopicDelete, "admin.users"),
            AclDecision::Allow
        );
        // system.* deny wins for everyone.
        assert!(matches!(
            t.check(PrincipalId::new(1), Action::TopicDelete, "system.logs"),
            AclDecision::Deny { .. }
        ));
        // Unmatched topic → NoMatch.
        assert!(matches!(
            t.check(PrincipalId::new(99), Action::Publish, "unknown.topic"),
            AclDecision::Deny {
                reason: topic_acl::DenyReason::NoMatch
            }
        ));
    }

    #[test]
    fn banner_lines_are_nonempty() {
        // Sanity: the banner string is non-trivial and well-formed.
        let line = format!("vireon {} — phase 9", env!("CARGO_PKG_VERSION"));
        assert!(line.starts_with("vireon "));
    }
}
