from typing import List, Dict, Any, Tuple
from datetime import datetime
import time
from decimal import Decimal

import psycopg2 as db
from psycopg2.extras import execute_values
from psycopg2.extras import RealDictCursor

# store all maps across tables, sk = Surrogate Key, pk = Primary Key
all_pk_sk_map = {}

# store all release date 
date_sk_map = {}

def load_dim(
    oltp_conn: db.extensions.connection,
    olap_conn: db.extensions.connection,
    oltp_table: str,
    olap_dim_table: str,
    olap_business_pk_name: str
) -> Dict[int, int]:    
    print(f"\n--- Fill dim: {olap_dim_table} ---")
    
    # read oltp table
    oltp_cursor = oltp_conn.cursor()
    oltp_cursor.execute(f"select id, name from {oltp_table}")
    oltp_data = oltp_cursor.fetchall()
    oltp_cursor.close()

    olap_insert_sql = f"""
        insert into {olap_dim_table} ({olap_business_pk_name}, name) 
        values (%s, %s)
        on conflict ({olap_business_pk_name}) do nothing
        returning id;
    """
    
    pk_sk_map: Dict[int, int] = {}
    
    with olap_conn.cursor() as olap_cursor:
        for business_pk, name in oltp_data:
            olap_cursor.execute(olap_insert_sql, (business_pk, name))
            res = olap_cursor.fetchone()
            if res:
                pk_sk_map[business_pk] = res[0]
            else:
                # to handle the duplicated record to avoid confusing, 
                select_duplicated_sql = f"""
                    select id from {olap_dim_table}
                    where
                    {olap_business_pk_name} = %s
                """
                olap_cursor.execute(select_duplicated_sql, (business_pk,))
                existed_sk = olap_cursor.fetchone()
                if existed_sk:
                    pk_sk_map[business_pk] = existed_sk[0]

    olap_conn.commit()
    print(f"Loaded {len(pk_sk_map)} rows for {olap_dim_table}.")
    return pk_sk_map


def load_time_dim(
    oltp_conn: db.extensions.connection,
    olap_conn: db.extensions.connection,
) -> Dict[int, int]:
    global date_sk_map

    oltp_cursor = oltp_conn.cursor()
    oltp_date_select_sql = "select release_date from games"
    oltp_cursor.execute(oltp_date_select_sql)
    oltp_dates_res = oltp_cursor.fetchall()

    # print(len(dates_res), dates_res[0][0])

    with olap_conn.cursor() as olap_cursor:
        for item in oltp_dates_res:
            current_date: datetime = item[0]
            olap_insert_sql = f"""
                insert into 
                dim_date (full_date, year, month, day) 
                values (%s, %s, %s, %s)
                on conflict (full_date) do nothing
                returning id
            """
            olap_date_value = (current_date, current_date.year, current_date.month, current_date.day)
            olap_cursor.execute(olap_insert_sql, olap_date_value)
            res = olap_cursor.fetchone()
            if res:
                date_sk_map[current_date] = res[0]
            else:
                select_existed_sql = "select id from dim_date where full_date = %s"
                olap_cursor.execute(select_existed_sql, (current_date, ))
                exited_date = olap_cursor.fetchone()
                if exited_date:
                    date_sk_map[current_date] = exited_date[0]

    olap_conn.commit()
    print(f"Loaded {len(date_sk_map)} dates")


def load_game_dim(
    oltp_conn: db.extensions.connection,
    olap_conn: db.extensions.connection,
) -> Dict[int, int]:
    print(f"\n--- Fill dim: game ---")
    oltp_cursor = oltp_conn.cursor(cursor_factory=RealDictCursor)
    oltp_game_sql = """
        select 
            id, game_code, game_name, required_age, price, 
            support_url, support_email, user_score, positive, negative, 
            average_playtime_forever, median_playtime_forever, 
            estimated_owners_min, estimated_owners_max
        from games;
    """
    oltp_cursor.execute(oltp_game_sql)
    oltp_game_res = oltp_cursor.fetchall()
    oltp_cursor.close()

    olap_game_insert_sql = """
        insert into dim_game (
            game_id, game_code, game_name, required_age, price, 
            support_url, support_email, user_score, positive, negative, 
            average_playtime_forever, median_playtime_forever, 
            estimated_owners_min, estimated_owners_max
        ) 
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (game_id) do nothing
        returning id;
    """

    game_sk_map: Dict[int, int] = {}
    with olap_conn.cursor() as olap_cursor:
        for row in oltp_game_res:
            oltp_game_id = row["id"]
            olap_data = (
                oltp_game_id, 
                row["game_code"], 
                row["game_name"], 
                row["required_age"], 
                row["price"], 
                row["support_url"],
                row["support_email"], 
                row["user_score"], 
                row["positive"], 
                row["negative"], 
                row["average_playtime_forever"], 
                row["median_playtime_forever"],
                row["estimated_owners_min"],
                row["estimated_owners_max"]
            )
            olap_cursor.execute(olap_game_insert_sql, olap_data)
            res = olap_cursor.fetchone()
            if res:
                game_sk_map[oltp_game_id] = res[0]
            else:
                olap_cursor.execute(f"select id from dim_game where game_id=%s", (oltp_game_id, ))
                existed_sk = olap_cursor.fetchone()
                if existed_sk:
                    game_sk_map[oltp_game_id] = existed_sk[0]

    print("FIlled game")
    olap_conn.commit()
    return game_sk_map


def load_bridge(
    oltp_conn: db.extensions.connection,
    olap_conn: db.extensions.connection,
    oltp_bridge_table: str,
    olap_bridge_table: str,
    fk_1_col_name: str,
    fk_2_col_name: str,
    sk_1_col_name: str,
    sk_2_col_name: str,
    sk_map_1: Dict[int, int],
    sk_map_2: Dict[int, int]
):
    pass
    print(f"\n--- Loading Bridge Table: {olap_bridge_table} ---")
    
    oltp_cursor = oltp_conn.cursor()
    oltp_cursor.execute(f"select {fk_1_col_name}, {fk_2_col_name} from {oltp_bridge_table}")
    oltp_data = oltp_cursor.fetchall()
    oltp_cursor.close()

    bridge_data: List[Tuple[int, int]] = []
    for id_1, id_2 in oltp_data:
        sk1 = sk_map_1.get(id_1)
        sk2 = sk_map_2.get(id_2)
        
        if sk1 is not None and sk2 is not None:
            bridge_data.append((sk1, sk2))

    insert_query = f"""
        insert into {olap_bridge_table} ({sk_1_col_name}, {sk_2_col_name}) 
        values (%s, %s)
        on conflict ({sk_1_col_name}, {sk_2_col_name}) do nothing;
    """
    
    with olap_conn.cursor() as olap_cursor:
        db.extras.execute_batch(olap_cursor, insert_query, bridge_data)
        
    olap_conn.commit()
    print(f"Loaded {len(bridge_data)} rows.")
    


def load_fact(
    oltp_conn: db.extensions.connection,
    olap_conn: db.extensions.connection,
    game_sk_map: Dict[int, int],
    date_sk_map: Dict[Any, int]
):
    print("\n--- Fill Fact Table ---")
    oltp_cursor = oltp_conn.cursor()
    oltp_cursor.execute("""
        select id, release_date, estimated_owners_min, estimated_owners_max, price 
        from games;
    """)
    oltp_data = oltp_cursor.fetchall()
    oltp_cursor.close()

    fact_data: List[Tuple[int, int, Decimal, Decimal]] = []
    for oltp_id, release_date, owners_min, owners_max, price in oltp_data:
        game_sk = game_sk_map[oltp_id]
        date_sk = date_sk_map[release_date]
        if game_sk is not None and date_sk is not None:
            min_rev = Decimal(round(owners_min * price, 2))
            max_rev = Decimal(round(owners_max * price, 2))
            fact_data.append((game_sk, date_sk, min_rev, max_rev))

    insert_query = """
        insert into fact_revenue (game_sk, date_sk, estimated_revenue_min, estimated_revenue_max)
        values (%s, %s, %s, %s);
    """
    with olap_conn.cursor() as olap_cursor:
        db.extras.execute_batch(olap_cursor, insert_query, fact_data)
        
    olap_conn.commit()
    print(f"Loaded {len(fact_data)} rows into fact_revenue.")




def main():

    global all_pk_sk_map

    conn_oltp = db.connect(
        host="localhost",
        database="steam_games",
        user="user",
        password="12345678",
        port=5432
    )
    conn_olap = db.connect(
        host="localhost",
        database="steam_games_olap",
        user="user",
        password="12345678",
        port=5432
    )

    load_time_dim(conn_oltp, conn_olap)

    all_pk_sk_map["languages"] = load_dim(conn_oltp, conn_olap, "languages", "dim_language", "language_id")
    all_pk_sk_map["genres"] = load_dim(conn_oltp, conn_olap, "genres", "dim_genre", "genre_id")
    all_pk_sk_map["categories"] = load_dim(conn_oltp, conn_olap, "categories", "dim_category", "category_id")
    all_pk_sk_map["games"] = load_game_dim(conn_oltp, conn_olap)

    load_bridge(
        conn_oltp, conn_olap,
        "game_languages", "bridge_game_language",
        "game_id", "language_id",
        "game_sk", "language_sk",
        all_pk_sk_map["games"], all_pk_sk_map["languages"]
    )
    load_bridge(
        conn_oltp, conn_olap,
        "game_genres", "bridge_game_genre",
        "game_id", "genres_id",
        "game_sk", "genre_sk",
        all_pk_sk_map["games"], all_pk_sk_map["genres"]
    )
    load_bridge(
        conn_oltp, conn_olap,
        "game_categories", "bridge_game_category",
        "game_id", "category_id",
        "game_sk", "category_sk",
        all_pk_sk_map["games"], all_pk_sk_map["categories"]
    )

    load_fact(
        conn_oltp, conn_olap,
        all_pk_sk_map["games"], date_sk_map
    )

    conn_oltp.close()
    conn_olap.close()

if __name__ == "__main__":
    main()


    