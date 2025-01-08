from pyspark.sql import SparkSession

def do_actors_history_scd(spark, actors_df, actor_films_df, actors_history_df): 
    # query = """
    #     WITH last_year AS (
    #     SELECT * FROM actors
    #     WHERE current_year = 2020
    # ), this_year as (
    #     SELECT * FROM actor_films
    #     WHERE year = 2021
    # ), this_year_film_rating AS (
    #     SELECT actorid, 
    #     actor,
    #     year, 
    #     ARRAY_AGG(ROW(
    #         ty.film,
    #         ty.rating,
    #         ty.rating,
    #         ty.filmid)::films) AS current_films, 
    #         AVG(rating) AS average_rating
    #         FROM this_year AS ty
    #         GROUP BY actorid, actor, year
    # )
    # SELECT 
    #     COALESCE(ly.actorid, ty.actorid) AS actorid,
    #     COALESCE(ly.actor, ty.actor) AS actor, 
    #     COALESCE(ly.current_year + 1, ty.year) AS current_year, 
    #     CASE 
    #         WHEN ly.current_year IS NULL THEN ty.current_films
    #         WHEN ty.year IS NULL THEN ly.films
    #         ELSE ly.films || ty.current_films
    #     END::films[] AS films, 
    #     CASE
    #         WHEN ty.average_rating IS NULL THEN ly.quality_class 
    #         ELSE 
    #             CASE 
    #                 WHEN ty.average_rating > 8 THEN 'star'
    #                 WHEN ty.average_rating > 7 THEN 'good'
    #                 WHEN ty.average_rating > 6 THEN 'average'
    #                 ELSE 'bad'
    #             END::quality_class
    #     END::quality_class AS quality_class, 
    #             CASE
    #                 WHEN ty IS NULL 
    #                 THEN False
    #                 ELSE True 
    #             END AS is_active
    #             FROM this_year_film_rating AS ty
    #             FULL OUTER JOIN last_year AS ly
    #             ON ly.actorid = ty.actorid 
    # """
    query = """
        WITH last_year AS (
            SELECT * FROM actors
            WHERE current_year = 2020
        ), this_year AS (
            SELECT * FROM actor_films
            WHERE year = 2021
        ), this_year_film_rating AS (
            SELECT 
                ty.actorid, 
                ty.filmid,
                ty.film,
                ty.votes,
                ty.rating,
                ty.year,
                ARRAY(COLLECT_LIST(NAMED_STRUCT(
                    'film', ty.film,
                    'votes', ty.votes,
                    'rating', ty.rating,
                    'filmid', ty.filmid
                ))) AS current_films, 
                AVG(rating) AS average_rating
            FROM this_year AS ty
            GROUP BY ty.actorid, ty.filmid, ty.film, ty.year, ty.votes, ty.rating
        )
        SELECT 
            COALESCE(ly.actorid, ty.actorid) AS actorid,
            COALESCE(ly.actor, ty.actor) AS actor, 
            COALESCE(ly.current_year + 1, ty.year) AS current_year, 
            CASE 
                WHEN ly.current_year IS NULL THEN ty.current_films
                WHEN ty.year IS NULL THEN ly.films
                ELSE CONCAT(ly.films, ty.current_films)
            END AS films, 
            CASE
                WHEN ty.average_rating IS NULL THEN 'unknown'
                ELSE 
                    CASE 
                        WHEN ty.average_rating > 8 THEN 'star'
                        WHEN ty.average_rating > 7 THEN 'good'
                        WHEN ty.average_rating > 6 THEN 'average'
                        ELSE 'bad'
                    END
            END AS quality_class, 
            CASE
                WHEN ty IS NULL THEN False
                ELSE True 
            END AS is_active
        FROM this_year_film_rating AS ty
        FULL OUTER JOIN last_year AS ly
        ON ly.actorid = ty.actorid 
    """
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")
    actors_history_df.createOrReplaceTempView("actors_history_scd")
    return spark.sql(query)
    

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_history_scd") \
        .getOrCreate()
    output_df = do_actors_history_scd(spark, spark.table("actors"), spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actors_history_scd")
