# KSQLDB Exercises

## Exercise 1: Create a Stream

Create a stream named 'user_events' from the topic 'user-events' with columns id (key), name, action. Assume the topic has JSON messages.

### Correction

```sql
CREATE STREAM user_events (
  id VARCHAR KEY,
  name VARCHAR,
  action VARCHAR
) WITH (
  KAFKA_TOPIC = 'user-events',
  VALUE_FORMAT = 'JSON'
);
```

## Exercise 2: Filter and Transform

Create a new stream 'login_events' from 'user_events' that only includes events where action is 'login', and transform the name to uppercase.

### Correction

```sql
CREATE STREAM login_events AS
SELECT id, UPPER(name) AS name, action
FROM user_events
WHERE action = 'login'
EMIT CHANGES;
```

## Exercise 3: Aggregate into a Table

Create a table 'user_action_counts' that counts the number of actions per user from 'user_events'.

### Correction

```sql
CREATE TABLE user_action_counts AS
SELECT id, COUNT(*) AS action_count
FROM user_events
GROUP BY id
EMIT CHANGES;
```

## Exercise 4: Windowed Aggregation

Create a table that counts user actions per hour using tumbling windows.

### Correction

```sql
CREATE TABLE hourly_user_actions AS
SELECT id,
       COUNT(*) AS action_count,
       WINDOWSTART AS window_start,
       WINDOWEND AS window_end
FROM user_events
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY id
EMIT CHANGES;
```

## Exercise 5: Stream-Table Join

Create a stream that joins user_events with user_profiles (assuming user_profiles is a table).

### Correction

```sql
CREATE STREAM enriched_events AS
SELECT e.id, e.action, p.name, p.email
FROM user_events e
LEFT JOIN user_profiles p ON e.id = p.id
EMIT CHANGES;
```

## Exercise 6: Materialized View with Filtering

Create a table that aggregates only 'login' actions per user.

### Correction

```sql
CREATE TABLE login_counts AS
SELECT id, COUNT(*) AS login_count
FROM user_events
WHERE action = 'login'
GROUP BY id
EMIT CHANGES;