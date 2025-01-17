from chispa.dataframe_comparer import *

from ..jobs.actors_history_scd import do_actors_history_scd
from collections import namedtuple

Actors = namedtuple("Actors", "actorid actor films current_year") 
ActorFilms = namedtuple("ActorFilms", "actorid, actor, film, votes, rating, filmid, year")
ActorsHistorySCD = namedtuple("ActorsHistorySCD", "actorid actor current_year films")
Films = namedtuple("Films", "film votes rating filmid")


UserDevicesCumulated = namedtuple("UserDevicesCumulated", "user_id device_id browser_type date device_activity_datelist")

def test_when_only_new_actors(spark): 
    # Setup
    actor_films_data = [
        ActorFilms(actorid=1, actor="Actor 1", film="Film 1", votes=100, rating=8.0, filmid=1, year=2001), 
        ActorFilms(actorid=1, actor="Actor 2", film="Film 2", votes=200, rating=9.0, filmid=2, year=2001), 
    ]

    expected_values = [
        ActorsHistorySCD(actorid=1, actor="Actor 1", current_year=2001, films=[Films(film="Film 1",votes=100, rating=8.0, filmid=1)]),
        ActorsHistorySCD(actorid=2, actor="Actor 2", current_year=2001, films=[Films(film="Film 2",votes=200, rating=9.0, filmid=2)])
    ]

    actors_df = spark.createDataFrame([], "actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")

    actor_films_df = spark.createDataFrame(actor_films_data, "actorid INT, actor STRING, film STRING, votes INT, rating DOUBLE, filmid INT, year INT")
    actors_history_scd_df = spark.createDataFrame([], "actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")
    expected_df = spark.createDataFrame(expected_values, "actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")
    
    #Actual 
    actual_df = do_actors_history_scd(spark, actors_df, actor_films_df, actors_history_scd_df) 

    actual_df.show()
    expected_df.show()

    #Assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)



