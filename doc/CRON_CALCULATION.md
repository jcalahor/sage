# Calculating next_run_at from Cron Expressions

## Overview

The `next_run_at` field is calculated from the cron expression and represents the **next time** the scheduled task should execute. This is precomputed and stored in the database for efficient querying.

## How It Works

### 1. Parse Cron Expression

Using the `cron` crate in Rust:

```rust
use cron::Schedule;
use chrono::{DateTime, Utc};
use std::str::FromStr;

// Parse cron expression
let cron_expr = "*/5 * * * *"; // Every 5 minutes
let schedule = Schedule::from_str(cron_expr)?;

// Get next occurrence from now
let now = Utc::now();
let next_run = schedule.after(&now).next().unwrap();

println!("Next run at: {}", next_run);
// Output: Next run at: 2025-12-19 12:10:00 UTC
```

### 2. With Timezone Support

```rust
use chrono_tz::Tz;
use chrono::TimeZone;

fn calculate_next_run(
    cron_expr: &str,
    timezone: &str,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    // Parse cron expression
    let schedule = Schedule::from_str(cron_expr)?;
    
    // Parse timezone
    let tz: Tz = timezone.parse()?;
    
    // Get current time in the specified timezone
    let now_local = Utc::now().with_timezone(&tz);
    
    // Get next occurrence in local timezone
    let next_local = schedule
        .after(&now_local)
        .next()
        .ok_or("No future occurrence found")?;
    
    // Convert back to UTC for storage
    let next_utc = next_local.with_timezone(&Utc);
    
    Ok(next_utc)
}

// Example usage
let next = calculate_next_run("0 9 * * MON-FRI", "America/Bogota")?;
// This will give you the next weekday at 9 AM Colombia time, stored as UTC
```

### 3. Complete Implementation Example

```rust
use cron::Schedule;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use std::str::FromStr;

pub struct CronCalculator;

impl CronCalculator {
    /// Calculate the next run time from a cron expression
    /// 
    /// # Arguments
    /// * `cron_expr` - Cron expression (e.g., "*/5 * * * *")
    /// * `timezone` - IANA timezone (e.g., "America/Bogota", "UTC")
    /// * `from_time` - Calculate from this time (None = now)
    /// 
    /// # Returns
    /// Next execution time in UTC
    pub fn next_run(
        cron_expr: &str,
        timezone: &str,
        from_time: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
        // Parse cron expression
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| format!("Invalid cron expression: {}", e))?;
        
        // Parse timezone
        let tz: Tz = timezone.parse()
            .map_err(|_| format!("Invalid timezone: {}", timezone))?;
        
        // Get the reference time
        let ref_time = from_time.unwrap_or_else(Utc::now);
        let ref_time_local = ref_time.with_timezone(&tz);
        
        // Get next occurrence
        let next_local = schedule
            .after(&ref_time_local)
            .next()
            .ok_or("No future occurrence found for this cron expression")?;
        
        // Convert to UTC
        Ok(next_local.with_timezone(&Utc))
    }
    
    /// Validate if a cron expression is valid
    pub fn validate(cron_expr: &str) -> Result<(), String> {
        Schedule::from_str(cron_expr)
            .map_err(|e| format!("Invalid cron expression: {}", e))?;
        Ok(())
    }
    
    /// Get multiple upcoming executions
    pub fn next_n_runs(
        cron_expr: &str,
        timezone: &str,
        count: usize,
    ) -> Result<Vec<DateTime<Utc>>, Box<dyn std::error::Error>> {
        let schedule = Schedule::from_str(cron_expr)?;
        let tz: Tz = timezone.parse()?;
        let now_local = Utc::now().with_timezone(&tz);
        
        let runs: Vec<DateTime<Utc>> = schedule
            .after(&now_local)
            .take(count)
            .map(|dt| dt.with_timezone(&Utc))
            .collect();
        
        Ok(runs)
    }
}
```

## Usage in Scheduled Task Creation

### When Creating a Schedule

```rust
// API receives request
POST /api/v1/schedules
{
  "cron_expression": "0 */2 * * *",  // Every 2 hours
  "timezone": "America/Bogota"
}

// Server calculates next_run_at
let next_run_at = CronCalculator::next_run(
    "0 */2 * * *",
    "America/Bogota",
    None, // From now
)?;

// Insert into database
INSERT INTO scheduled_tasks (
    id, 
    cron_expression, 
    timezone,
    next_run_at,  // ← Precomputed!
    ...
) VALUES (
    uuid_generate_v4(),
    '0 */2 * * *',
    'America/Bogota',
    '2025-12-19 14:00:00+00',  // Stored in UTC
    ...
)
```

### When Scheduler Executes a Task

```rust
// 1. Scheduler finds due tasks (efficient query!)
SELECT * FROM scheduled_tasks
WHERE enabled = true 
  AND next_run_at <= NOW()  -- ← Simple comparison!
ORDER BY priority DESC, next_run_at ASC
LIMIT 100;

// 2. For each task, execute it
for task in due_tasks {
    // Create and publish task...
    
    // 3. Calculate NEXT next_run_at
    let new_next_run = CronCalculator::next_run(
        &task.cron_expression,
        &task.timezone,
        Some(task.next_run_at), // From last scheduled time
    )?;
    
    // 4. Update the schedule
    UPDATE scheduled_tasks
    SET last_run_at = NOW(),
        next_run_at = $1  -- ← New precomputed time!
    WHERE id = $2;
}
```

## Examples with Real Cron Expressions

### Example 1: Every 5 Minutes

```rust
let cron = "*/5 * * * *";
let tz = "UTC";

// If now is 12:03 PM
// Next run: 12:05 PM
// After that: 12:10 PM
// After that: 12:15 PM

let runs = CronCalculator::next_n_runs(cron, tz, 5)?;
for (i, run) in runs.iter().enumerate() {
    println!("Run {}: {}", i + 1, run);
}
```

Output:
```
Run 1: 2025-12-19 12:05:00 UTC
Run 2: 2025-12-19 12:10:00 UTC
Run 3: 2025-12-19 12:15:00 UTC
Run 4: 2025-12-19 12:20:00 UTC
Run 5: 2025-12-19 12:25:00 UTC
```

### Example 2: Daily at 2 AM (with timezone)

```rust
let cron = "0 2 * * *";
let tz = "America/Bogota"; // UTC-5

// If now is Dec 19, 2025 12:00 PM Bogota time
// Next run: Dec 20, 2025 2:00 AM Bogota = Dec 20, 2025 7:00 AM UTC

let next = CronCalculator::next_run(cron, tz, None)?;
println!("Next run (UTC): {}", next);
println!("Next run (Bogota): {}", next.with_timezone(&tz));
```

Output:
```
Next run (UTC): 2025-12-20 07:00:00 UTC
Next run (Bogota): 2025-12-20 02:00:00 -05:00
```

### Example 3: Weekdays at 9 AM

```rust
let cron = "0 9 * * MON-FRI";
let tz = "America/Bogota";

// If today is Thursday 12:00 PM
// Next run: Friday 9:00 AM
// After that: Monday 9:00 AM (skips weekend!)

let runs = CronCalculator::next_n_runs(cron, tz, 5)?;
```

## Why Precompute next_run_at?

### ✅ Benefits

1. **Fast Queries**: Simple `WHERE next_run_at <= NOW()` vs parsing cron on every check
2. **Indexed**: Can create index on `next_run_at` for O(log n) lookups
3. **Reliable**: Calculated once, stored, no parsing errors during execution
4. **Timezone Aware**: Conversion done once, stored in UTC

### Query Performance Comparison

**❌ Without Precomputation** (BAD):
```sql
-- Would need to parse cron and calculate in SQL or app layer EVERY time
SELECT * FROM scheduled_tasks WHERE enabled = true;
-- Then in app: filter by parsing each cron expression (SLOW!)
```

**✅ With Precomputation** (GOOD):
```sql
-- Direct timestamp comparison, uses index
SELECT * FROM scheduled_tasks 
WHERE enabled = true AND next_run_at <= NOW()
ORDER BY priority DESC;
-- Index scan: O(log n) - FAST!
```

## Edge Cases

### 1. Missed Executions (Misfire)

If scheduler was down and `next_run_at` is in the past:

```rust
if task.next_run_at < Utc::now() {
    // Option 1: Skip missed run, calculate from now
    let next = CronCalculator::next_run(
        &task.cron_expression,
        &task.timezone,
        None, // From now
    )?;
    
    // Option 2: Execute now, then calculate next from scheduled time
    execute_task(&task).await?;
    let next = CronCalculator::next_run(
        &task.cron_expression,
        &task.timezone,
        Some(task.next_run_at), // From missed time
    )?;
}
```

### 2. Daylight Saving Time

The `chrono-tz` crate handles DST automatically:

```rust
// Spring forward: 2:00 AM becomes 3:00 AM
let cron = "0 2 * * *"; // Daily at 2 AM
let tz = "America/New_York";

// On DST transition day, will correctly skip the non-existent 2:00 AM hour
let next = CronCalculator::next_run(cron, tz, None)?;
```

### 3. Invalid Next Occurrence

Some cron expressions may not have future occurrences:

```rust
// February 31st doesn't exist
let cron = "0 0 31 2 *"; // Invalid!

match CronCalculator::next_run(cron, "UTC", None) {
    Ok(next) => println!("Next: {}", next),
    Err(e) => println!("Error: {}", e), // "No future occurrence found"
}
```

## Testing

### Unit Tests Example

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_every_5_minutes() {
        let cron = "*/5 * * * *";
        let tz = "UTC";
        
        // Test from a specific time
        let from = Utc.ymd(2025, 12, 19).and_hms(12, 3, 0);
        let next = CronCalculator::next_run(cron, tz, Some(from)).unwrap();
        
        // Should be 12:05
        assert_eq!(next.hour(), 12);
        assert_eq!(next.minute(), 5);
    }
    
    #[test]
    fn test_timezone_conversion() {
        let cron = "0 9 * * *"; // 9 AM
        let tz = "America/Bogota"; // UTC-5
        
        let next = CronCalculator::next_run(cron, tz, None).unwrap();
        
        // Should be stored as UTC (14:00 UTC = 9:00 Bogota)
        let bogota_tz: Tz = "America/Bogota".parse().unwrap();
        let next_local = next.with_timezone(&bogota_tz);
        assert_eq!(next_local.hour(), 9);
    }
    
    #[test]
    fn test_invalid_cron() {
        let result = CronCalculator::validate("invalid cron");
        assert!(result.is_err());
    }
}
```

## Database Migration

```sql
-- Add helper function to validate cron in PostgreSQL (optional)
CREATE OR REPLACE FUNCTION validate_cron_expression(expr TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    -- Basic validation (can be enhanced)
    RETURN expr ~ '^[0-9\*\-\,\/]+\s+[0-9\*\-\,\/]+\s+[0-9\*\-\,\/]+\s+[0-9\*\-\,\/]+\s+[0-9\*\-\,\/A-Z]+$';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Add check constraint
ALTER TABLE scheduled_tasks 
ADD CONSTRAINT valid_cron_expression 
CHECK (validate_cron_expression(cron_expression));
```

## Summary

| Step | Action | Where | When |
|------|--------|-------|------|
| 1. Parse | Validate cron expression | API endpoint | Schedule creation |
| 2. Calculate | Compute `next_run_at` from cron + timezone | API endpoint | Schedule creation |
| 3. Store | Save `next_run_at` in database (UTC) | Database | Schedule creation |
| 4. Query | `SELECT WHERE next_run_at <= NOW()` | Scheduler loop | Every 30 seconds |
| 5. Execute | Create task, publish to Kafka | Scheduler loop | When due |
| 6. Recalculate | Compute next `next_run_at` | Scheduler loop | After execution |
| 7. Update | Store new `next_run_at` | Database | After execution |

The key insight: **Calculate once, query efficiently!** 🚀
