from pyspark.sql.functions import *
from datetime import date

#Current date
today = date.today()

df = spark \
.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.option("delimiter", ",") \
.load("s3://datalake-pedrosa-524095156763/staging/*")

#Creating Temp View
df.createOrReplaceTempView("reservations")

# Transformations
df = spark.sql("""
select
*,
case
  when type_of_meal_plan = 'Meal Plan 1' then 1
  when type_of_meal_plan = 'Meal Plan 2' then 2
  when type_of_meal_plan = 'Meal Plan 3' then 3
  else 99
end as Meal_Type,
case
  when booking_status = 'Not_Canceled' then 'Not Canceled'
  else 'Canceled'
end as booking_status_result,
right(room_type_reserved,1) as Room_Type
from reservations
""")

df.createOrReplaceTempView("reservations")

# Concat date columns
df = df.withColumn("Date", concat_ws("-",df.arrival_date,df.arrival_month,df.arrival_year))
df.createOrReplaceTempView("reservations")

# Convert Column Names
df = spark.sql("""
  select
    Booking_ID,
    no_of_adults as No_Adults,
    no_of_children as No_Children,
    no_of_weekend_nights as No_Weekend_Nights,
    no_of_week_nights as No_Week_Nights,
    Meal_Type,
    required_car_parking_space as No_Car_Parking,
    Room_Type,
    lead_time as Lead_Time,
    Date as Arrival_Date,
    arrival_year as Arrival_Year,
    market_segment_type as Market_Segment_Type,
    repeated_guest as Repeated_Guest,
    no_of_previous_cancellations as No_Previous_Cancellations,
    no_of_previous_bookings_not_canceled as No_Previous_Bookings,
    avg_price_per_room as Avg_Price_Per_Room,
    no_of_special_requests as No_Special_Requests,
    booking_status_result as Booking_Status
  from reservations
""")

# Convert string column to date
df = df.withColumn("Arrival_Date", to_date("Arrival_Date", "dd-MM-yyyy"))

#Save in parquet format
df \
.write \
.mode("overwrite") \
.format("parquet") \
.partitionBy("Arrival_Year") \
.save("s3://datalake-pedrosa-524095156763/raw/hotel-reservations/data/dataload={}/".format(today))



