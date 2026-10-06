use crate::{SqliteStore, error::map_sqlx, sql};
use dmt_store::StoreError;
use sqlx::{
    FromRow, Sqlite, Transaction,
    query::{Query, QueryAs},
    sqlite::{SqliteArguments, SqliteRow},
};

/// Owns the transaction; all statements in a mutation pass through this boundary.
pub(crate) struct WriteTx {
    tx: Transaction<'static, Sqlite>,
    #[cfg(feature = "test-faults")]
    faults: std::sync::Arc<crate::faults::FaultState>,
    #[cfg(feature = "test-faults")]
    statements: usize,
}
impl WriteTx {
    pub(crate) async fn begin(store: &SqliteStore) -> Result<Self, StoreError> {
        Ok(Self {
            #[cfg(feature = "test-faults")]
            faults: store.faults.clone(),
            #[cfg(feature = "test-faults")]
            statements: 0,
            tx: store
                .writer
                .begin_with(sql::BEGIN_IMMEDIATE)
                .await
                .map_err(map_sqlx)?,
        })
    }
    pub(crate) async fn execute(
        &mut self,
        query: Query<'_, Sqlite, SqliteArguments>,
    ) -> Result<u64, StoreError> {
        let affected = query
            .execute(&mut *self.tx)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        #[cfg(feature = "test-faults")]
        self.after_statement().await?;
        Ok(affected)
    }
    pub(crate) async fn fetch_optional<O>(
        &mut self,
        query: QueryAs<'_, Sqlite, O, SqliteArguments>,
    ) -> Result<Option<O>, StoreError>
    where
        O: Send + Unpin + for<'r> FromRow<'r, SqliteRow>,
    {
        let row = query
            .fetch_optional(&mut *self.tx)
            .await
            .map_err(map_sqlx)?;
        #[cfg(feature = "test-faults")]
        self.after_statement().await?;
        Ok(row)
    }
    pub(crate) async fn fetch_all<O>(
        &mut self,
        query: QueryAs<'_, Sqlite, O, SqliteArguments>,
    ) -> Result<Vec<O>, StoreError>
    where
        O: Send + Unpin + for<'r> FromRow<'r, SqliteRow>,
    {
        let rows = query.fetch_all(&mut *self.tx).await.map_err(map_sqlx)?;
        #[cfg(feature = "test-faults")]
        self.after_statement().await?;
        Ok(rows)
    }
    #[cfg(feature = "test-faults")]
    async fn after_statement(&mut self) -> Result<(), StoreError> {
        self.statements += 1;
        self.faults.after_statement(self.statements).await
    }
    pub(crate) async fn commit(self) -> Result<(), StoreError> {
        self.tx.commit().await.map_err(map_sqlx)
    }
}
