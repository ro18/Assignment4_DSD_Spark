import os
from itertools import permutations
from dotenv import load_dotenv
from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession

### install dependency ###
# pip install python-dotenv
# pip install pyspark # make sure you have jdk installed
#####################################

### please update your relative path while running your code ###
temp_airline_textfile = r"C:\Code\DSD\Assignment4_DSD_Spark\flights_data_c.txt"
temp_airline_csvfile = r"path/to/flights_data.csv"
#default_spark_context = "local[*]"  # only update if you need
#######################################


### please don't update these lines ###
#load_dotenv()
airline_textfile = os.getenv("AIRLINE_TXT_FILE", temp_airline_textfile)
airline_csvfile = os.getenv("AIRLINE_CSV_FILE", temp_airline_csvfile)
#spark_context = os.getenv("SPARK_CONTEXT", default_spark_context)
#######################################


def co_occurring_airline_pairs_by_origin(flights_data: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the flights_data from txt file. Performs a series of MapReduce operations via PySpark
    to calculate the number of co-occurring airlines with same origin airports operating on the same date, determine count of such occurrences pairwise.
    Returns the results as an RDD sorted by the airline pairs alphabetically ascending (by first and then second value in the pair) with the counts in descending order.
    :param flights_dat: RDD object of the contents of flights_data
    :return: RDD of pairs of airline and the number of occurrences
        Example output:     [((Airline_A,Airline_B),3),
                                ((Airline_A,Airline_C),2),
                                ((Airline_B,Airline_C),1)]
    """
  
    flights_mapped = flights_data.map(lambda x:  x.split(','))
    
    print("1")
    print(flights_mapped.collect())
    
    
    
    
    flight_pairs = flights_mapped.map(lambda flight: ((flight[0], flight[2]), flight[1]))
    
    print("2")
    print(flight_pairs.collect())
    
#     flight_pairs_count = flight_pairs.flatMap(lambda x: [(tupple, 1) for tupple in x])

    
#     print("3")
#     print(flight_pairs_count.collect())
    
    
    
    flight_pair_addded=flight_pairs.groupByKey().mapValues(list).collect()
    
    print("3")
    print(flight_pair_addded)
    
    # flight_reduced = flight_pairs.reduceByKey(lambda x, y: x)


# # Group flights by date and origin
# grouped_flights = flight_pairs.groupByKey()
#     #flight_origin = flights_mapped.reduceByKey(lambda x, y: x+y)
    
#     print(flight_origin.collect())
                                  
                                              


    


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to
    Chicago, IL.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing.
    """
    raise NotImplementedError("Your Implementation Here.")


def main():
    # initialize SparkContext and SparkSession
    #sc = SparkContext(spark_context)

    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.getOrCreate()

    print("########################## Problem 1 ########################")
    # problem 1: co-occurring operating flights with Spark and MapReduce
    # read the file
    flights_data = sc.textFile(airline_textfile)
    sorted_airline_pairs = co_occurring_airline_pairs_by_origin(flights_data)
    sorted_airline_pairs.persist()
    for pair, count in sorted_airline_pairs.take(10):
        print(f"{pair}: {count}")

    print("########################## Problem 2 ########################")
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv(airline_csvfile, header=True, inferSchema=True)
    print(
        "Q1:",
        air_flights_most_canceled_flights(flights),
        "had the most canceled flights in September 2021.",
    )
    print(
        "Q2:",
        air_flights_diverted_flights(flights),
        "flights were diverted between the period of 20th-30th " "November 2021.",
    )
    print(
        "Q3:",
        air_flights_avg_airtime(flights),
        "is the average airtime for flights that were flying from "
        "Nashville to Chicago.",
    )
    print(
        "Q4:",
        air_flights_missing_departure_time(flights),
        "unique dates where departure time (DepTime) was " "not recorded.",
    )


if __name__ == "__main__":
    main()