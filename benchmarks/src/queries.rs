//! Query definitions for the e-commerce benchmark schema.
//!
//! Uses simple_query (text protocol) instead of extended protocol with prepared
//! statements for compatibility with transaction-mode connection poolers like
//! PgBouncer. Named prepared statements are server-side state that doesn't work
//! when the pooler can route transactions to different backend connections.

use anyhow::Result;
use deadpool_postgres::Object as Client;
use rand::seq::SliceRandom;
use rand::Rng;

use crate::params::QueryParams;

/// Execute a "browse products" query using simple query protocol.
pub async fn browse_products(client: &Client, params: &QueryParams) -> Result<u64> {
    let category = params.categories.choose(&mut rand::thread_rng());
    let offset: i64 = rand::thread_rng().gen_range(0..5) * 20;

    let query = if let Some(cat) = category {
        format!(
            "SELECT id, sku, name, price, category
             FROM products
             WHERE is_active = true AND category = '{}'
             ORDER BY created_at DESC
             LIMIT 20 OFFSET {}",
            cat.replace('\'', "''"), // Escape single quotes
            offset
        )
    } else {
        format!(
            "SELECT id, sku, name, price, category
             FROM products
             WHERE is_active = true
             ORDER BY created_at DESC
             LIMIT 20 OFFSET {}",
            offset
        )
    };

    let results = client.simple_query(&query).await?;
    let row_count = results.iter().filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    Ok(row_count as u64)
}

/// Execute a "view product detail" query using simple query protocol.
pub async fn view_product(client: &Client, params: &QueryParams) -> Result<u64> {
    let product_id = {
        let mut rng = rand::thread_rng();
        params.product_ids.choose(&mut rng).copied()
    };
    if let Some(product_id) = product_id {
        let query = format!("SELECT * FROM products WHERE id = {}", product_id);
        let results = client.simple_query(&query).await?;
        let row_count = results.iter().filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))).count();
        Ok(row_count as u64)
    } else {
        Ok(0)
    }
}

/// Execute a "search products" query using simple query protocol.
pub async fn search_products(client: &Client, _params: &QueryParams) -> Result<u64> {
    let search_terms = ["laptop", "mouse", "keyboard", "monitor", "headset", "wireless", "gaming", "Product"];
    let term = {
        let mut rng = rand::thread_rng();
        *search_terms.choose(&mut rng).unwrap_or(&"laptop")
    };

    let query = format!(
        "SELECT id, sku, name, price
         FROM products
         WHERE is_active = true AND name ILIKE '%{}%'
         LIMIT 10",
        term.replace('\'', "''")
    );
    let results = client.simple_query(&query).await?;
    let row_count = results.iter().filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    Ok(row_count as u64)
}

/// Execute a "check order history" query using simple query protocol.
pub async fn order_history(client: &Client, params: &QueryParams) -> Result<u64> {
    let user_id = {
        let mut rng = rand::thread_rng();
        params.user_ids.choose(&mut rng).copied()
    };
    if let Some(user_id) = user_id {
        let query = format!(
            "SELECT o.id, o.order_number, o.status, o.total_amount, o.created_at
             FROM orders o
             WHERE o.user_id = {}
             ORDER BY o.created_at DESC
             LIMIT 5",
            user_id
        );
        let results = client.simple_query(&query).await?;
        let row_count = results.iter().filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))).count();
        Ok(row_count as u64)
    } else {
        Ok(0)
    }
}

/// Execute a "view order details" query using simple query protocol.
pub async fn order_details(client: &Client, params: &QueryParams) -> Result<u64> {
    let order_id = {
        let mut rng = rand::thread_rng();
        params.order_ids.choose(&mut rng).copied()
    };
    if let Some(order_id) = order_id {
        let query = format!(
            "SELECT oi.*, p.name as product_name
             FROM order_items oi
             JOIN products p ON p.id = oi.product_id
             WHERE oi.order_id = {}",
            order_id
        );
        let results = client.simple_query(&query).await?;
        let row_count = results.iter().filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))).count();
        Ok(row_count as u64)
    } else {
        Ok(0)
    }
}

/// Query type with associated weight for random selection.
#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    BrowseProducts,
    ViewProduct,
    SearchProducts,
    OrderHistory,
    OrderDetails,
}

impl QueryType {
    /// Get all query types with their weights (must sum to 100).
    pub fn weighted_all() -> Vec<(Self, u8)> {
        vec![
            (Self::BrowseProducts, 42),
            (Self::ViewProduct, 26),
            (Self::SearchProducts, 16),
            (Self::OrderHistory, 11),
            (Self::OrderDetails, 5),
        ]
    }

    /// Select a random query type based on weights.
    pub fn random() -> Self {
        let weighted = Self::weighted_all();
        let total: u8 = weighted.iter().map(|(_, w)| w).sum();
        let mut rng = rand::thread_rng();
        let mut pick = rng.gen_range(0..total);

        for (qt, weight) in weighted {
            if pick < weight {
                return qt;
            }
            pick -= weight;
        }

        Self::BrowseProducts
    }

    /// Execute this query type and return row count.
    pub async fn execute(&self, client: &Client, params: &QueryParams) -> Result<u64> {
        match self {
            Self::BrowseProducts => browse_products(client, params).await,
            Self::ViewProduct => view_product(client, params).await,
            Self::SearchProducts => search_products(client, params).await,
            Self::OrderHistory => order_history(client, params).await,
            Self::OrderDetails => order_details(client, params).await,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::BrowseProducts => "browse_products",
            Self::ViewProduct => "view_product",
            Self::SearchProducts => "search_products",
            Self::OrderHistory => "order_history",
            Self::OrderDetails => "order_details",
        }
    }
}
