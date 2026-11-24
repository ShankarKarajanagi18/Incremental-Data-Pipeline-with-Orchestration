-- ============================================================================
-- revenue_analysis.sql
-- Financial and Revenue Analytics Queries for Amazon Athena
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Total Gross and Net Revenue by Currency and Normalized USD
-- Compares original transactional currency vs normalized USD revenue.
-- ----------------------------------------------------------------------------
SELECT 
    currency,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_original_amount,
    ROUND(SUM(amount_usd), 2) AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2) AS avg_booking_usd,
    ROUND(MIN(amount_usd), 2) AS min_booking_usd,
    ROUND(MAX(amount_usd), 2) AS max_booking_usd
FROM travel_bookings_db.processed_bookings
WHERE booking_status IN ('CONFIRMED', 'COMPLETED')
GROUP BY currency
ORDER BY total_revenue_usd DESC;


-- ----------------------------------------------------------------------------
-- Query 2: Daily Ingestion & Booking Revenue Trends
-- Analyzes day-over-day financial volume using partition columns.
-- ----------------------------------------------------------------------------
SELECT 
    year,
    month,
    day,
    CONCAT(year, '-', month, '-', day) AS booking_day,
    COUNT(*) AS total_bookings,
    SUM(CASE WHEN booking_status IN ('CONFIRMED', 'COMPLETED') THEN 1 ELSE 0 END) AS confirmed_bookings,
    ROUND(SUM(CASE WHEN booking_status IN ('CONFIRMED', 'COMPLETED') THEN amount_usd ELSE 0.0 END), 2) AS net_revenue_usd,
    ROUND(AVG(CASE WHEN booking_status IN ('CONFIRMED', 'COMPLETED') THEN amount_usd ELSE NULL END), 2) AS avg_booking_usd
FROM travel_bookings_db.processed_bookings
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC;


-- ----------------------------------------------------------------------------
-- Query 3: Top 10 Revenue-Generating Travel Destinations
-- Identifies highest-yielding destinations and market share.
-- ----------------------------------------------------------------------------
SELECT 
    destination,
    COUNT(*) AS total_bookings,
    ROUND(SUM(amount_usd), 2) AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2) AS avg_ticket_price_usd,
    ROUND(100.0 * SUM(amount_usd) / SUM(SUM(amount_usd)) OVER(), 2) AS pct_of_total_revenue
FROM travel_bookings_db.processed_bookings
WHERE booking_status IN ('CONFIRMED', 'COMPLETED')
GROUP BY destination
ORDER BY total_revenue_usd DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- Query 4: Payment Channel Performance and Revenue Share
-- Evaluates which payment channels capture highest volume & value.
-- ----------------------------------------------------------------------------
SELECT 
    payment_method,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount_usd), 2) AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2) AS avg_ticket_size_usd,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS transaction_share_pct
FROM travel_bookings_db.processed_bookings
WHERE booking_status IN ('CONFIRMED', 'COMPLETED')
GROUP BY payment_method
ORDER BY total_revenue_usd DESC;
