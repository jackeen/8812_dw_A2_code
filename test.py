import re
import psycopg2 as db


def language_clean():
    text = """
        English
        Russian
        Spanish - Spain
        Japanese
        Czech
        Japanese (all with full audio support)
        Japanese 

        [b][/b] 
        K'iche'
        Traditional Chinese
        Simplified Chinese
        Spanish - Latin America
        Portuguese - Brazil
        English Dutch  English
    """
    matches = re.findall(r"[A-Z][a-z]+", text)
    print(set(matches))


def main():
    conn = db.connect(
        host="localhost",
        database="steam_games",
        user="user",
        password="12345678",
        port=5432
    )
    cursor = conn.cursor()
    query_select = f"SELECT id, name FROM languages WHERE name = ANY(%s)"
    ls = set(["Polish","English"])
    cursor.execute(query_select, (list(ls), ))
    rows = cursor.fetchall()
    for r in rows:
        print(r[0], r[1])

    conn.close()




if __name__ == "__main__":
    main()