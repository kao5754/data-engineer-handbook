from pyspark.sql import SparkSession

def do_users_devices_cumulated_datelist(spark, events_df, devices_df, user_devices_cumulated_df): 
    """
    Generates a cumulative date list for user devices based on events and device data.

    Args:
        spark (SparkSession): The Spark session object.
        events_df (DataFrame): DataFrame containing event data.
        devices_df (DataFrame): DataFrame containing device data.
        user_devices_cumulated_df (DataFrame): DataFrame with cumulated user-device data.

    Returns:
        DataFrame: Updated DataFrame with cumulative device activity date list.
    """
    query = """
    WITH today AS (
    SELECT e.user_id, 
        DATE(e.event_time) as date, 
        e.device_id, 
        d.browser_type, 
        ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, d.browser_type ORDER BY e.event_time) AS row_num
    FROm events e
    LEFT JOIN devices AS d
    ON e.device_id = d.device_id
    WHERE DATE(e.event_time) = ('2023-01-02')
    AND e.user_id IS NOT NULL 
    AND d.device_id IS NOT NULL 
    )
    , today_deduped AS (
    SELECT * FROM today
    WHERE row_num = 1
    ) 
    , yesterday AS (
        SELECT * FROM user_devices_cumulated
        WHERE date = DATE('2023-01-01')
    )
    SELECT COALESCE(t.user_id, y.user_id) AS user_id, 
    COALESCE(t.device_id, y.device_id) AS device_id, 
    COALESCE(t.browser_type, y.browser_type) AS browser_type, 
    COALESCE(t.date, y.date + 1) AS date, 
    CASE 
        WHEN y.device_activity_datelist IS NULL 
        THEN ARRAY(t.date)
        WHEN t.date IS NULL 
        THEN y.device_activity_datelist 
        ELSE y.device_activity_datelist || array(t.date)
    END AS device_activity_datelist	
    FROM today_deduped t
    FULL OUTER JOIN yesterday y 
    ON t.user_id = y.user_id
    AND t.device_id = y.device_id
    AND t.browser_type = y.browser_type
    """
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")
    events_df.createOrReplaceTempView("events")
    devices_df.createOrReplaceTempView("devices")
    return spark.sql(query)
    

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("user_devices_cumulated_datelist") \
        .getOrCreate()
    output_df = do_users_devices_cumulated_datelist(spark, spark.table("events"), spark.table("devices"), spark.table("users_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")
