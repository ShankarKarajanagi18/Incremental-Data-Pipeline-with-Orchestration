-- ============================================================================
-- booking_analysis.sql
-- Operational, Customer, and Quality Analytics Queries for Amazon Athena
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Booking Lifecycle Status Breakdown & Cancellation Loss
-- Evaluates conversion, cancellation rate, and financial loss from cancellations.
-- ----------------------------------------------------------------------------
SELECT 
    booking_status,
    COUNT(*) AS booking_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS status_share_pct,
    ROUND(SUM(amount_usd), 2) AS total_status_volume_usd,
    ROUND(AVG(amount_usd), 2) AS avg_amount_usd
FROM travel_bookings_db.processed_bookings
GROUP BY booking_status
ORDER BY booking_count DESC;


-- ----------------------------------------------------------------------------
-- Query 2: Lead Time Distribution and Travel Window Analysis
-- Analyzes how early customers book their trips before travel date.
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN lead_time_days < 7 THEN '01. Last Minute (< 7 days)'
        WHEN lead_time_days BETWEEN 7 AND 30 THEN '02. Short Term (1-4 weeks)'
        WHEN lead_time_days BETWEEN 31 AND 90 THEN '03. Medium Term (1-3 months)'
        ELSE '04. Long Term (> 3 months)'
    END AS lead_time_bracket,
    COUNT(*) AS total_bookings,
    ROUND(SUM(amount_usd), 2) AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2) AS avg_ticket_usd,
    ROUND(100.0 * SUM(CASE WHEN booking_status = 'CANCELLED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate_pct
FROM travel_bookings_db.processed_bookings
GROUP BY 
    CASE 
        WHEN lead_time_days < 7 THEN '01. Last Minute (< 7 days)'
        WHEN lead_time_days BETWEEN 7 AND 30 THEN '02. Short Term (1-4 weeks)'
        WHEN lead_time_days BETWEEN 31 AND 90 THEN '03. Medium Term (1-3 months)'
        ELSE '04. Long Term (> 3 months)'
    END
ORDER BY lead_time_bracket;


-- ----------------------------------------------------------------------------
-- Query 3: High-Value Repeat Customer Identification
-- Identifies top customers by transaction frequency and gross booking value.
-- ----------------------------------------------------------------------------
SELECT 
    customer_id,
    COUNT(*) AS completed_trips,
    COUNT(DISTINCT destination) AS distinct_destinations_visited,
    ROUND(SUM(amount_usd), 2) AS lifetime_value_usd,
    ROUND(AVG(amount_usd), 2) AS avg_spend_per_trip_usd,
    MIN(booking_date) AS first_booking_timestamp,
    MAX(booking_date) AS most_recent_booking_timestamp
FROM travel_bookings_db.processed_bookings
WHERE booking_status IN ('CONFIRMED', 'COMPLETED')
GROUP BY customer_id
ORDER BY lifetime_value_usd DESC
LIMIT 15;


-- ----------------------------------------------------------------------------
-- Query 4: Data Quality & Dead-Letter Quarantine Audit
-- Aggregates error codes from the quarantined table to measure data hygiene.
-- ----------------------------------------------------------------------------
SELECT 
    validation_error_codes,
    COUNT(*) AS defect_occurrences,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS defect_share_pct
FROM travel_bookings_db.quarantined_bookings
GROUP BY validation_error_codes
ORDER BY defect_occurrences DESC;
