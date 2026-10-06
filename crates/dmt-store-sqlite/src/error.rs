use dmt_store::StoreError;

// Owned errors match Result::map_err at every sqlx call site.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn map_sqlx(error: sqlx::Error) -> StoreError {
    // SQLITE_BUSY is primary result code 5; extended codes retain its low byte.
    const SQLITE_BUSY: u32 = 5;
    let busy = match &error {
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::Database(db) => db
            .code()
            .and_then(|code| code.parse::<u32>().ok())
            .is_some_and(|code| code & 0xff == SQLITE_BUSY),
        _ => false,
    };
    if busy {
        StoreError::Busy
    } else {
        StoreError::Backend(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pool_timeout_is_busy() {
        assert!(matches!(
            map_sqlx(sqlx::Error::PoolTimedOut),
            StoreError::Busy
        ));
    }
    #[test]
    fn row_not_found_is_backend() {
        assert!(matches!(
            map_sqlx(sqlx::Error::RowNotFound),
            StoreError::Backend(_)
        ));
    }
}
