import os, json
import psycopg

DB_URL = os.environ["DATABASE_URL"]  # строка подключения Supabase, со ?sslmode=require

def safe_load(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
        
def upsert(cur, key, data):
    cur.execute(
        "insert into kvstore (k, v) values (%s, %s::jsonb) "
        "on conflict (k) do update set v=excluded.v, updated_at=now()",
        (key, json.dumps(data, ensure_ascii=False)),
    )

with psycopg.connect(DB_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            create table if not exists kvstore(
                k text primary key,
                v jsonb not null,
                updated_at timestamptz not null default now()
            );
        """)
        upsert(cur, "readings", safe_load("readings.json"))
        upsert(cur, "tariffs",  safe_load("tariffs.json"))
        upsert(cur, "state",    safe_load("state.json"))
        conn.commit()

print("Done: files -> Supabase kvstore")
