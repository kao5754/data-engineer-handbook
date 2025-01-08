from chispa.dataframe_comparer import *

from ..jobs.user_devices_cumulated_datelist import do_users_devices_cumulated_datelist
from collections import namedtuple
from datetime import datetime

Events = namedtuple("Events", "user_id event_time device_id")
Devices = namedtuple("Devices", "device_id browser_type")
UserDevicesCumulated = namedtuple("UserDevicesCumulated", "user_id device_id browser_type date device_activity_datelist")

def test_when_no_previous_user_cumulated_data(spark): 
    event_data = [
        Events(user_id=1, event_time="2023-01-02 00:00:00", device_id=1), 
        Events(user_id=2, event_time="2023-01-02 00:00:00", device_id=2),
        Events(user_id=3, event_time="2023-01-02 00:00:00", device_id=3)
    ]

    device_data = [
        Devices(device_id=1, browser_type="chrome"),
        Devices(device_id=2, browser_type="firefox"),
        Devices(device_id=3, browser_type="safari")
    ]

    event_df = spark.createDataFrame(event_data)
    device_df = spark.createDataFrame(device_data)
    user_devices_cumulated_df = spark.createDataFrame([], "user_id INT, device_id INT, browser_type STRING, date DATE, device_activity_datelist ARRAY<DATE>")

    current_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()

    actual_df = do_users_devices_cumulated_datelist(spark, event_df, device_df, user_devices_cumulated_df)
    expected_values = [
        UserDevicesCumulated(user_id=1, device_id=1, browser_type="chrome", date= current_date, device_activity_datelist=[ current_date]),
        UserDevicesCumulated(user_id=2, device_id=2, browser_type="firefox", date= current_date, device_activity_datelist=[current_date]),
        UserDevicesCumulated(user_id=3, device_id=3, browser_type="safari", date= current_date, device_activity_datelist=[current_date])
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_when_duplicate_rows(spark): 
    event_data = [
        Events(user_id=1, event_time="2023-01-02 00:00:00", device_id=1), 
        Events(user_id=1, event_time="2023-01-02 00:00:00", device_id=1),
        Events(user_id=1, event_time="2023-01-02 00:00:00", device_id=1)
    ]

    device_data = [
        Devices(device_id=1, browser_type="chrome")
    ]

    event_df = spark.createDataFrame(event_data)
    device_df = spark.createDataFrame(device_data)
    user_devices_cumulated_df = spark.createDataFrame([], "user_id INT, device_id INT, browser_type STRING, date DATE, device_activity_datelist ARRAY<DATE>")

    actual_df = do_users_devices_cumulated_datelist(spark, event_df, device_df, user_devices_cumulated_df)
    current_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()
    expected_values = [
        UserDevicesCumulated(user_id=1, device_id=1, browser_type="chrome", date= current_date, device_activity_datelist=[current_date])
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_when_new_data_for_specific_user_device_browswer(spark): 
    #Arrange
    previous_date = datetime.strptime("2023-01-01", "%Y-%m-%d").date()
    current_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()

    event_data = [
        Events(user_id=1, event_time="2023-01-02 00:00:00", device_id=1)
    ]

    device_data = [
        Devices(device_id=1, browser_type="chrome")
    ]

    user_devices_cumulated_data = [
        UserDevicesCumulated(user_id=1, device_id=1, browser_type="chrome", date= previous_date, device_activity_datelist=[previous_date]),
        UserDevicesCumulated(user_id=2, device_id=2, browser_type="firefox", date= previous_date, device_activity_datelist=[previous_date]),
        UserDevicesCumulated(user_id=3, device_id=3, browser_type="safari", date= previous_date, device_activity_datelist=[previous_date])
    ]
    
    expected_data = [
        UserDevicesCumulated(user_id=1, device_id=1, browser_type="chrome", date=current_date, device_activity_datelist=[previous_date]),
        UserDevicesCumulated(user_id=2, device_id=2, browser_type="firefox", date=current_date, device_activity_datelist=[previous_date]),
        UserDevicesCumulated(user_id=3, device_id=3, browser_type="safari", date=current_date, device_activity_datelist=[previous_date])
    ]

    event_df = spark.createDataFrame(event_data)
    device_df = spark.createDataFrame(device_data)
    user_devices_cumulated_df = spark.createDataFrame(user_devices_cumulated_data, "user_id INT, device_id INT, browser_type STRING, date DATE, device_activity_datelist ARRAY<DATE>")
    expected_df = spark.createDataFrame(expected_data)
    #Actual  
    actual_df = do_users_devices_cumulated_datelist(spark, event_df, device_df, user_devices_cumulated_df)

    #Assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

