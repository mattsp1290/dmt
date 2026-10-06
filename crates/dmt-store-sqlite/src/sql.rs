pub(crate) const SELECT_SCHEMA_VERSION: &str =
    "SELECT value FROM dmt_schema_meta WHERE key = 'schema_version'";
pub(crate) const PRAGMA_JOURNAL_MODE: &str = "PRAGMA journal_mode";
