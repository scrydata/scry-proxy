//! Parameter bootstrapping - load valid IDs from database at startup.

use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use std::sync::Arc;

/// Cached parameters for query generation.
#[derive(Debug, Clone)]
pub struct QueryParams {
    pub user_ids: Vec<i32>,
    pub product_ids: Vec<i32>,
    pub order_ids: Vec<i32>,
    pub categories: Vec<String>,
}

impl QueryParams {
    /// Bootstrap parameters from the database.
    pub async fn load(pool: &Pool) -> Result<Self> {
        let client = pool.get().await.context("Failed to get connection")?;

        let user_ids: Vec<i32> = client
            .query("SELECT id FROM users ORDER BY random() LIMIT 500", &[])
            .await
            .context("Failed to load user IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let product_ids: Vec<i32> = client
            .query(
                "SELECT id FROM products WHERE is_active = true ORDER BY random() LIMIT 500",
                &[],
            )
            .await
            .context("Failed to load product IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let order_ids: Vec<i32> = client
            .query("SELECT id FROM orders ORDER BY random() LIMIT 500", &[])
            .await
            .context("Failed to load order IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let categories: Vec<String> = client
            .query("SELECT DISTINCT category FROM products WHERE category IS NOT NULL", &[])
            .await
            .context("Failed to load categories")?
            .iter()
            .map(|row| row.get("category"))
            .collect();

        Ok(Self { user_ids, product_ids, order_ids, categories })
    }

    pub fn is_valid(&self) -> bool {
        !self.user_ids.is_empty() && !self.product_ids.is_empty()
    }
}

pub type SharedParams = Arc<QueryParams>;
