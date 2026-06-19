// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! MQTT topic wildcard matching per §4.7.

/// Reports whether `filter` matches `topic` per MQTT spec §4.7.
///
/// `filter` may contain `+` (single-level wildcard) and `#` (multi-level
/// wildcard, must be last). Topics beginning with `$` are not matched by
/// bare wildcards at the first level, per §4.7.2.
//fusa:req REQ-WILD-001
//fusa:req REQ-WILD-002
//fusa:req REQ-WILD-003
//fusa:req REQ-WILD-004
//fusa:req REQ-WILD-005
//fusa:req REQ-WILD-006
//fusa:req REQ-WILD-007
//fusa:req REQ-WILD-008
pub fn match_topic(filter: &str, topic: &str) -> bool {
    // MQTT §4.7.3: zero-length topic or filter is not permitted.
    if filter.is_empty() || topic.is_empty() {
        return false;
    }

    // Exact match short-circuit.
    if filter == topic {
        return true;
    }

    let topic_is_system = topic.starts_with('$');

    // '#' alone — matches all non-system topics.
    if filter == "#" {
        return !topic_is_system;
    }

    // 'filter/subtree/#' — matches filter/subtree and anything beneath it.
    if let Some(prefix) = filter.strip_suffix("/#") {
        if topic_is_system && !prefix.starts_with('$') {
            return false;
        }
        return topic == prefix || topic.starts_with(&format!("{}/", prefix));
    }

    // No '#' — match level-by-level with '+' as single-level wildcard.
    let f_parts: Vec<&str> = filter.split('/').collect();
    let t_parts: Vec<&str> = topic.split('/').collect();

    if f_parts.len() != t_parts.len() {
        return false;
    }

    for (i, (f, t)) in f_parts.iter().zip(t_parts.iter()).enumerate() {
        if *f == "+" {
            // '+' at the first level does not match '$' topics.
            if i == 0 && topic_is_system {
                return false;
            }
        } else if f != t {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(match_topic("a/b/c", "a/b/c"));
    }

    #[test]
    fn hash_matches_all_non_system() {
        assert!(match_topic("#", "a/b/c"));
        assert!(match_topic("#", "a"));
        assert!(!match_topic("#", "$SYS/broker"));
    }

    #[test]
    fn hash_suffix() {
        assert!(match_topic("a/#", "a"));
        assert!(match_topic("a/#", "a/b"));
        assert!(match_topic("a/#", "a/b/c"));
        assert!(!match_topic("a/#", "b/c"));
    }

    #[test]
    fn plus_single_level() {
        assert!(match_topic("a/+/c", "a/b/c"));
        assert!(match_topic("a/+/c", "a/z/c"));
        assert!(!match_topic("a/+/c", "a/b/d"));
        assert!(!match_topic("a/+/c", "a/b/c/d"));
    }

    #[test]
    fn plus_first_level_no_system() {
        assert!(!match_topic("+/topic", "$SYS/topic"));
    }

    #[test]
    fn system_topic_exact_match() {
        assert!(match_topic("$SYS/broker", "$SYS/broker"));
    }

    #[test]
    fn no_match_different_lengths() {
        assert!(!match_topic("a/b", "a/b/c"));
    }

    #[test]
    fn empty_level_preserved() {
        assert!(match_topic("a//b", "a//b"));
        assert!(!match_topic("a//b", "a/b"));
    }

    #[test]
    fn hash_system_subtree() {
        assert!(match_topic("$SYS/#", "$SYS/broker/version"));
    }

    #[test]
    fn plus_multi_level_no_match() {
        assert!(!match_topic("+", "a/b"));
    }
}
