import ijson
import os
from datetime import datetime
import time

import psycopg2 as db
from psycopg2.extras import execute_values

import cleaner
from etl_log import InitLogger, LogLevel, LogStatus


# data collection depend on game code
languages_game_dict = dict()
developers_game_dict = dict()
publishers_game_dict = dict()
categories_game_dict = dict()
genres_game_dict = dict()

# the cache of stored props and ids
languages_ids_dict = None
developers_ids_dict = None
publishers_ids_dict = None
categories_ids_dict = None
genres_ids_dict = None
platforms_ids_dict = None

# fixed data for platforms
platform_set = set(["windows", "mac", "linux"])


def convert_date(logger: InitLogger, game_code, date_str):
    try:
        return datetime.strptime(date_str, "%b %d, %Y").date()
    except ValueError as e:
        pass

    try:
        date_obj = datetime.strptime(date_str, "%b %Y").date().replace(day=1)
        logger.push_log(game_code, "release_date", date_str, LogLevel.LOW, LogStatus.AUTO_FIXED)
        return date_obj
    except ValueError as e:
        logger.push_log(game_code, "release_date", date_str, LogLevel.MIDDLE, LogStatus.NEED_REVIEW)
        return None

    


def main():
    conn = db.connect(
        host="localhost",
        database="steam_games",
        user="user",
        password="12345678",
        port=5432
    )
    cursor = conn.cursor()

    # record the clean log
    logger = InitLogger()

    json_data = "./data/games.json"
    count = 0
    if os.path.exists(json_data):
        with open(json_data, "r", encoding="utf-8") as file:
            # collect props and insert them all in db
            insert_props_into_db(logger, cursor, file)
            
            # reset the position of file
            file.seek(0)
            
            # collect games and store it and the relationships 
            start = time.time()
            for k, v in ijson.kvitems(file, ""):
                count += 1
                insert_games_into_db(logger, cursor, k, v)
            
            end = time.time()
            print(f"game table finished during {end - start:.4f} seconds, {count} items are treated")
    
    cursor.close()
    conn.commit()
    conn.close()

    logger.store_logs()
    logger.finish()



def insert_props_into_db(logger: InitLogger, cursor, file):
    start = time.time()
    count = 0

    global languages_game_dict
    global developers_game_dict
    global publishers_game_dict
    global categories_game_dict
    global genres_game_dict

    global languages_ids_dict
    global developers_ids_dict
    global publishers_ids_dict
    global categories_ids_dict
    global genres_ids_dict
    global platforms_ids_dict
    
    languages_set = set()
    developers_set = set()
    publishers_set = set()
    categories_set = set()
    genres_set = set()

    languages_one_game_set = None
    developers_one_game_set = None
    publishers_one_game_set = None
    categories_one_game_set = None
    genres_one_game_set = None

    
    # collect all props of games
    for game_code, game in ijson.kvitems(file, ""):
        count += 1
        # collect languages
        supported_languages = game.get("supported_languages", [])
        audio_languages = game.get("full_audio_languages", [])

        languages_one_game_set = set()
        for lang_str in supported_languages:
            ls = cleaner.parse_languages(lang_str)
            if len(ls) > 0:
                logger.push_log(game_code, "languages", lang_str, LogLevel.LOW, LogStatus.AUTO_FIXED)
            languages_one_game_set = languages_one_game_set.union(ls)
        for lang_str in audio_languages:
            ls = cleaner.parse_languages(lang_str)
            if len(ls) > 0:
                logger.push_log(game_code, "languages", lang_str, LogLevel.LOW, LogStatus.AUTO_FIXED)
            languages_one_game_set = languages_one_game_set.union(ls)
        
        languages_set = languages_set.union(languages_one_game_set)
        languages_game_dict[game_code] = languages_one_game_set
        

        # collect developers
        developers = game.get("developers", [])
        developers_one_game_set = set()
        for dev in developers:
            developers_one_game_set.add(dev)
        developers_set = developers_set.union(developers_one_game_set)
        developers_game_dict[game_code] = developers_one_game_set

        # collect publishers
        publishers = game.get("publishers", [])
        publishers_one_game_set = set()
        for pub in publishers:
            pub = pub.strip()
            if pub != "":
                publishers_one_game_set.add(pub)
        publishers_set = publishers_set.union(publishers_one_game_set)
        publishers_game_dict[game_code] = publishers_one_game_set

        # collect categories
        categories = game.get("categories", [])
        categories_one_game_set = set()
        for cate in categories:
            categories_one_game_set.add(cate)
        categories_set = categories_set.union(categories_one_game_set)
        categories_game_dict[game_code] = categories_one_game_set

        # collect 
        genres = game.get("genres", [])
        genres_one_game_set = set()
        for genre in genres:
            genres_one_game_set.add(genre)
        genres_set = genres_set.union(genres_one_game_set)
        genres_game_dict[game_code] = genres_one_game_set


    lang_sql = f"insert into languages (name) values %s returning id"
    lang_arr = [(v,) for v in languages_set]
    lang_ids = execute_values(cursor, lang_sql, lang_arr, fetch=True)
    languages_ids_dict = dict(zip([v[0] for v in lang_arr], [v[0] for v in lang_ids]))
    print(f"languages finished with {len(languages_set)} records")
    
    dev_sql = f"insert into developers (name) values %s returning id"
    developers_arr = [(v,) for v in developers_set]
    developer_ids = execute_values(cursor ,dev_sql, developers_arr, fetch=True)
    developers_ids_dict = dict(zip([v[0] for v in developers_arr], [v[0] for v in developer_ids]))
    print(f"developers finished with {len(developers_set)} records")

    pub_sql = f"insert into publishers (name) values %s returning id"
    pub_arr = [(v,) for v in publishers_set]
    pub_ids = execute_values(cursor, pub_sql, pub_arr, fetch=True)
    publishers_ids_dict = dict(zip([v[0] for v in pub_arr], [v[0] for v in pub_ids]))
    print(f"publisher finished with {len(publishers_set)} records")

    cate_sql = f"insert into categories (name) values %s returning id"
    cate_arr = [(v,) for v in categories_set]
    cate_ids = execute_values(cursor, cate_sql, cate_arr, fetch=True)
    categories_ids_dict = dict(zip([v[0] for v in cate_arr], [v[0] for v in cate_ids]))
    print(f"categories finished with {len(categories_set)} records")

    genres_sql = f"insert into genres (name) values %s returning id"
    genres_arr = [(v,) for v in genres_set]
    genres_ids = execute_values(cursor, genres_sql, genres_arr, fetch=True)
    genres_ids_dict = dict(zip([v[0] for v in genres_arr], [v[0] for v in genres_ids]))
    print(f"genres finished with {len(genres_set)} records")

    # insert platforms, this is fixed windows, mac, linux
    platform_sql = f"insert into platforms (name) values %s returning id"
    platform_arr = [(v,) for v in platform_set]
    platform_ids = execute_values(cursor, platform_sql, platform_arr, fetch=True)
    platforms_ids_dict = dict(zip([v[0] for v in platform_arr], [v[0] for v in platform_ids]))
    print(f"platforms finished with {len(platform_set)} records")

    end = time.time()
    print(f"props table finished during {end - start:.4f} seconds, {count} items are treated")


def insert_games_into_db(logger: InitLogger, cursor, game_code, game):
    game_fields = {
        "game_code": game_code,
        "game_name": game.get("name"),
        "release_date": game.get("release_date"),
        "required_age": game.get("required_age"),
        "price": game.get("price"),
        "dlc_count": game.get("dlc_count"),
        "detailed_description": game.get("detailed_description"),
        "short_description": game.get("short_description"),
        "about_the_game": game.get("about_the_game"),
        "header_image": game.get("header_image"),
        "website": game.get("website"),
        "support_url": game.get("support_url"),
        "support_email": game.get("support_email"),
        "metacritic_score": game.get("metacritic_score") or None,
        "metacritic_url": game.get("metacritic_url"),
        "achievements": game.get("achievements"),
        "recommendations": game.get("recommendations") or None,
        "user_score": game.get("user_score") or None,
        "positive": game.get("positive") or None,
        "negative": game.get("negative") or None,
        "average_playtime_forever": game.get("average_playtime_forever") or None,
        "median_playtime_forever": game.get("median_playtime_forever") or None,
        "estimated_owners_min": None,
        "estimated_owners_max": None,
    }

    # clean and convert release date
    game_fields["release_date"] = convert_date(logger, game_code, game_fields.get("release_date"))

    # clean and convert owners
    game_owners_str = game.get("estimated_owners", "")
    if game_owners_str != "":
        game_owners_parts = game_owners_str.replace(" ", "").split("-")
        game_fields["estimated_owners_min"] = int(game_owners_parts[0])
        game_fields["estimated_owners_max"] = int(game_owners_parts[1])

    # insert games
    game_columns = ", ".join(game_fields.keys())
    game_placeholders = ", ".join(["%s"] * len(game_fields))
    game_sql = f"insert into games ({game_columns}) values ({game_placeholders}) returning id"
    cursor.execute(game_sql, list(game_fields.values()))
    game_id = cursor.fetchone()[0]
    
    # clean and insert review + original_text
    # review_str = game.get("reviews", "")
    # if review_str != "":
    #     review_json = cleaner.parse_reviews_by_llm(review_str)
    #     if review_json is None:
    #         logger.push_log(game_code, "reviews", review_str, LogLevel.LOW, LogStatus.NEED_REVIEW)
    #     else:
    #         for item in review_json:
    #             review_fields = {
    #                 "game_id": game_id,
    #                 "quote": item.get("quote"),
    #                 "source": item.get("source"),
    #                 "rating": item.get("rating"),
    #                 "out_of": item.get("out_of"),
    #             }
    #             review_sql = f"insert into reviews ({", ".join(review_fields.keys())}) values (%s,%s,%s,%s,%s)"
    #             try:
    #                 cursor.execute(review_sql, list(review_fields.values()))
    #             except ValueError as e:
    #                 logger.push_log(game_code, "reviews", review_str, LogLevel.MIDDLE, LogStatus.NEED_REVIEW)

    # insert package not implemented
    
    # connect game with languages
    languages_set = languages_game_dict[game_code]
    if len(languages_set) > 0:
        # langs_sql = f"SELECT id, name FROM languages WHERE name = ANY(%s)"
        # cursor.execute(langs_sql, (list(languages_set), ))
        # lang_rows = cursor.fetchall()
        # lang_game_relations = [(game_id, id) for id, name in lang_rows]
        
        languages_arr = [v for v in languages_set]
        languages_ids = []
        for v in languages_arr:
            languages_ids.append(languages_ids_dict[v])
        lang_game_relations = [(game_id, id) for id in languages_ids]

        if len(lang_game_relations) > 0:
            relation_sql = """
                insert into game_languages (game_id, language_id)
                values %s
                on conflict (game_id, language_id) do nothing;
            """
            execute_values(cursor, relation_sql, lang_game_relations)
    # print(f"Finished languages relationships")
    
    
    # connect game with genres
    genres_set = genres_game_dict[game_code]
    if len(genres_set) > 0:
        genres_arr = [v for v in genres_set]
        genres_ids = []
        for v in genres_arr:
            genres_ids.append(genres_ids_dict[v])
        genres_game_relations = [(game_id, id) for id in genres_ids]
        if len(genres_game_relations) > 0:
            relation_sql = """
                insert into game_genres (game_id, genres_id)
                values %s
                on conflict (game_id, genres_id) do nothing;
            """
            execute_values(cursor, relation_sql, genres_game_relations)
    # print(f"Finished genres relationships")


    # connect game with developers
    developers_set = developers_game_dict[game_code]
    if len(developers_set) > 0:
        developer_arr = [v for v in developers_set]
        developer_ids = []
        for v in developer_arr:
            developer_ids.append(developers_ids_dict[v])
        developer_game_relations = [(game_id, id) for id in developer_ids]
        if len(developer_game_relations) > 0:
            relation_sql = """
                insert into game_developers (game_id, developer_id)
                values %s
                on conflict (game_id, developer_id) do nothing;
            """
            execute_values(cursor, relation_sql, developer_game_relations)
    # print(f"Finished developers relationships")


    # connect game with publishers
    publishers_set = publishers_game_dict[game_code]
    if len(publishers_set) > 0:
        publisher_arr = [v for v in publishers_set]
        publisher_ids = []
        for v in publisher_arr:
            publisher_ids.append(publishers_ids_dict[v])
        publisher_game_relations = [(game_id, id) for id in publisher_ids]
        if len(publisher_game_relations) > 0:
            relation_sql = """
                insert into game_publishers (game_id, publisher_id)
                values %s
                on conflict (game_id, publisher_id) do nothing;
            """
            execute_values(cursor, relation_sql, publisher_game_relations)
    # print(f"Finished publishers relationships")

    
    # connect game with categories
    categories_set = categories_game_dict[game_code]
    if len(categories_set) > 0:
        category_arr = [v for v in categories_set]
        category_ids = []
        for v in category_arr:
            category_ids.append(categories_ids_dict[v])
        category_game_relations = [(game_id, id) for id in category_ids]
        if len(category_game_relations) > 0:
            relation_sql = """
                insert into game_categories (game_id, category_id)
                values %s
                on conflict (game_id, category_id) do nothing;
            """
            execute_values(cursor, relation_sql, category_game_relations)
    # print(f"Finished categories relationships")


    # connect game with platforms
    # data bugs ... wait to fix, every game has three platforms right now
    current_game_platform_list = []
    if game.get("windows"):
        current_game_platform_list.append("windows")
    if game.get("mac"):
        current_game_platform_list.append("mac")
    if game.get("linux"):
        current_game_platform_list.append("linux")
    
    if len(platform_set) > 0:
        platform_arr = [v for v in platform_set]
        platform_ids = []
        for v in platform_arr:
            if v in current_game_platform_list:
                platform_ids.append(platforms_ids_dict[v])
        platform_game_relations = [(game_id, id) for id in platform_ids]
        if len(platform_game_relations) > 0:
            relation_sql = """
                insert into game_platforms (game_id, platform_id)
                values %s
                on conflict (game_id, platform_id) do nothing;
            """
            execute_values(cursor, relation_sql, platform_game_relations)
    # print(f"Finished platforms relationships")



if __name__ == "__main__":
    main()