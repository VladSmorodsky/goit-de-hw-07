# goit-de-hw-07: Airflow DAG for counting Olympic medals

The `vs_medal_counts` DAG ([dags/medal_counts_dag.py](dags/medal_counts_dag.py)) picks a random medal type, counts its rows in `olympic_dataset.athlete_event_results` and writes the result to `vs_hw07.medal_counts`. A final sensor checks that the newest record is no older than 30 seconds.

| Task | What it does |
|---|---|
| `create_table` | Creates the `vs_hw07` schema and the `medal_counts` table (`IF NOT EXISTS`) |
| `pick_medal` | Picks `Bronze`, `Silver` or `Gold` |
| `pick_medal_task` | Runs the matching `calc_*` task and skips the others |
| `calc_Bronze` / `calc_Silver` / `calc_Gold` | Counts the medals and writes the count with the record's creation time |
| `generate_delay` | `time.sleep(delay_seconds)` |
| `check_for_correctness` | `SqlSensor`: the record must be at most 30 s old, otherwise the sensor fails on timeout |

## Requirements

- Docker Desktop with at least 4 GB of memory allocated.
- Access to the course MySQL server: host, login and password.

## Running

1. Create `.env` and put the MySQL host, login and password into `AIRFLOW_CONN_GOIT_MYSQL_DB`:
   ```bash
   cp .env.example .env
   ```
2. Initialize Airflow. This is needed once; wait for `exited with code 0`:
   ```bash
   docker compose up airflow-init
   ```
3. Start Airflow:
   ```bash
   docker compose up -d
   ```
4. Open http://localhost:8080 (login `airflow`, password `airflow`) and unpause `vs_medal_counts`.
5. Click ▶ Trigger DAG:
   - with the default `delay_seconds = 5` the sensor succeeds;
   - with `delay_seconds = 35` the sensor fails, because the record becomes older than 30 s.

## Checking the result

```sql
SELECT * FROM vs_hw07.medal_counts ORDER BY id DESC;
```

## Stopping

```bash
docker compose down      # keeps the run history
docker compose down -v   # wipes the Airflow metadata database
```
