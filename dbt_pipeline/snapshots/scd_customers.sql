-- ============================================================
-- SNAPSHOT: scd_customers (Slowly Changing Dimension Type 2)
-- ============================================================
-- PURPOSE: Track historical changes in customer loyalty_points.
--
-- PRINCIPLE — SCD Type 2:
--   Instead of OVERWRITING the old value when loyalty_points change,
--   we KEEP the old row (with a closed timestamp) and INSERT a new row.
--
--   This gives us a complete audit trail:
--   ┌─────────────┬────────────────┬──────────────┬───────────────┐
--   │ customer_id │ loyalty_points │ valid_from   │ valid_to      │
--   ├─────────────┼────────────────┼──────────────┼───────────────┤
--   │ 2824        │ 377            │ 2025-01-01   │ 2025-06-08    │ ← expired
--   │ 2824        │ 450            │ 2025-06-09   │ NULL          │ ← current
--   └─────────────┴────────────────┴──────────────┴───────────────┘
--
-- HOW IT WORKS:
--   - strategy = 'check': dbt compares the value of columns listed
--     in `check_cols` against what's already stored in the snapshot.
--   - If any check_col changed → close old row, insert new row.
--   - unique_key: what identifies "the same entity" (customer_id)
--
-- dbt automatically adds:
--   - dbt_scd_id       → unique row identifier
--   - dbt_updated_at   → when this row was last updated
--   - dbt_valid_from   → when this version became active
--   - dbt_valid_to     → when this version expired (NULL = current)
-- ============================================================

{% snapshot scd_customers %}

{{
    config(
        target_schema  = 'silver_transactions',
        unique_key     = 'customer_id',
        strategy       = 'check',
        check_cols     = ['loyalty_points'],
        invalidate_hard_deletes = True
    )
}}

-- Source is the dim_customers model (already deduplicated)
SELECT
    customer_id,
    loyalty_points
FROM {{ ref('dim_customers') }}

{% endsnapshot %}
