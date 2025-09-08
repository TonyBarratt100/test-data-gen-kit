import os
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# Database connection
PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = os.getenv("PGPORT", "5432")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGDATABASE = os.getenv("PGDATABASE", "hackathon_db")

def get_schema():
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def ask_gpt_about_schema(schema_rows):
    schema_text = "\n".join([f"{t}.{c} ({d})" for t, c, d in schema_rows])
    prompt = f"""
    You are a SQL analysis assistant.
    Given this schema:

    {schema_text}

    Suggest 3 useful SQL queries to check data quality,
    anomalies, or interesting business insights.
    """
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful SQL assistant."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=500,
    )
    return resp.choices[0].message.content

if __name__ == "__main__":
    schema = get_schema()
    suggestions = ask_gpt_about_schema(schema)
    print("=== GPT Suggestions ===")
    print(suggestions)
