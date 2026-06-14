use std::collections::HashSet;

/// Predicate that identifies which tables use merge-on-read CDC semantics and
/// which of those are additionally partitioned.
///
/// For tables in scope, CDC mutations become append-only rows annotated with
/// `_etl_version` and `_etl_deleted`. The view layer merges on read.
/// Tables not in this scope retain the default in-place update/delete behavior.
///
/// Partitioned tables (`public_lines`, `public_measurements`) require
/// partition-aware handling during compaction; non-partitioned tables in scope
/// (e.g. `public_observations`) do not.
#[derive(Clone, Debug, Default)]
pub struct MergeOnReadScope {
    tables: HashSet<String>,
    partitioned: HashSet<String>,
}

impl MergeOnReadScope {
    /// Builds a scope from an iterable of table names.
    ///
    /// Tables named `public_lines` or `public_measurements` are automatically
    /// marked as partitioned within the scope.
    pub fn from_tables<I, S>(t: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let tables: HashSet<String> = t.into_iter().map(Into::into).collect();
        let partitioned = tables
            .iter()
            .filter(|t| *t == "public_lines" || *t == "public_measurements")
            .cloned()
            .collect();
        Self { tables, partitioned }
    }

    /// Returns `true` if `t` is in the merge-on-read scope.
    pub fn contains(&self, t: &str) -> bool {
        self.tables.contains(t)
    }

    /// Returns `true` if `t` is in the merge-on-read scope and is partitioned.
    pub fn is_partitioned(&self, t: &str) -> bool {
        self.partitioned.contains(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_membership() {
        let s = MergeOnReadScope::from_tables(["public_lines", "public_observations"]);
        assert!(s.contains("public_lines"));
        assert!(s.contains("public_observations"));
        assert!(!s.contains("public_dimension__values"));
        assert!(s.is_partitioned("public_lines"));
        assert!(!s.is_partitioned("public_observations"));
    }
}
