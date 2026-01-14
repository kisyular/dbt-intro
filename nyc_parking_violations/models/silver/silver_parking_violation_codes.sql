-- This model transforms bronze parking violation codes into a normalized silver table
-- where each violation code has separate rows for Manhattan (96th St & below) vs all other areas
-- This structure makes it easier to join with parking violations based on location

-- CTE for Manhattan (96th Street and below) violation codes and fees
WITH manhattan_violation_codes AS (
    SELECT
        violation_code,                          -- Unique identifier for the violation type
        definition,                               -- Description of the violation
        TRUE AS is_manhattan_96th_st_below,      -- Flag indicating this row is for Manhattan pricing
        manhattan_96th_st_below AS fee_usd       -- Fine amount for Manhattan 96th St & below
    FROM
        {{ref('bronze_parking_violation_codes')}} -- References the bronze layer source table
),

-- CTE for all other areas (non-Manhattan or Manhattan above 96th St) violation codes and fees
all_other_violation_codes AS (
    SELECT
        violation_code,                          -- Unique identifier for the violation type
        definition,                               -- Description of the violation
        FALSE AS is_manhattan_96th_st_below,     -- Flag indicating this row is NOT for Manhattan pricing
        all_other_areas AS fee_usd               -- Fine amount for all other areas
    FROM
        {{ref('bronze_parking_violation_codes')}} -- References the bronze layer source table
)

-- Combine both CTEs to create a row for each violation code per location type
SELECT * FROM manhattan_violation_codes
UNION ALL                                         -- Preserves all rows including duplicates (faster than UNION)
SELECT * FROM all_other_violation_codes
ORDER BY violation_code ASC                       -- Sort results by violation code in ascending order