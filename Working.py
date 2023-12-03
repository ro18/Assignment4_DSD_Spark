
import os
from itertools import permutations
from dotenv import load_dotenv
from pyspark import RDD, SparkContext
from collections import Counter
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col,desc,lit


### install dependency ###
# pip install python-dotenv
# pip install pyspark # make sure you have jdk installed
#####################################

### please update your relative path while running your code ###
temp_airline_textfile = r'C:\Code\DSD\Assignment4_DSD_Spark\flights_data.txt'
temp_airline_csvfile = r"C:\Code\DSD\Assignment4_DSD_Spark\Combined_Flights_2021.csv"
temp_airline_csvfile = r"C:\Code\DSD\Assignment4_DSD_Spark\TestFlights.csv"


default_spark_context = "local[*]"  # only update if you need
#######################################


### please don't update these lines ###
load_dotenv()
airline_textfile = os.getenv("AIRLINE_TXT_FILE", temp_airline_textfile)
airline_csvfile = os.getenv("AIRLINE_CSV_FILE", temp_airline_csvfile)
spark_context = os.getenv("SPARK_CONTEXT", default_spark_context)
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
    
    #Output Type:[(str, str, str)]
    flights_mapped = flights_data.map(lambda x:  x.split(','))
    
    #Output Type:[(str, str, [str])]
    flight_pairs = flights_mapped.map(lambda flight: ((flight[0], flight[2]), [ flight[1] ]))
        
    def countLen(x):
        final_dict= Counter(x[1])      
        return (x[0], Counter(x[1]))

    #Output Type: [(str, str, [str,str ..])] 
    flight_reduced = flight_pairs.reduceByKey(lambda a, b: a + b)
    
    #Outpur Type:  [(str, str, Counter({str: int , str : int ...}))] 
    flight_reduced_count = flight_reduced.map(countLen)
        
    #Output Type:  [(str, str, Counter({str: int , str : int ...}))] 
    flight_filter_count = flight_reduced_count.filter(lambda x : len(x[1]) >1) ## filter rows with just one airline

    def getPairs(x):
        combination = []  
        
        final_pairs = {} 
        
        combination.extend(permutations(x[1].keys(), 2)) #get permutations of all items in counter list
        
        for index, element in enumerate(combination):
            
            if (element[1],element[0]) in final_pairs.keys(): #if reverse duplicate exists then skip it
                continue
            final_pairs[element] = min(x[1][element[0]],x[1][element[1]])
            
       
        return final_pairs
    
    #Output Type:  [(str, str): int , (str,str): 1] 
    flight_filtered_pairs = flight_filter_count.map(getPairs)

        
    #Output Type:  [((str, str), int) , ((str,str), 1) .. ] 
    airlines_keys_values = flight_filtered_pairs.flatMap(lambda x : [(k, v) for k, v in x.items()])

    #Output Type:  [((str, str), int) , ((str,str), 1) .. ]    
    airlines_keys_values_reduce = airlines_keys_values.reduceByKey(lambda x,y : x+y)
    
    #Output Type:  [((str, str), int) , ((str,str), 1) .. ]    
    airlines_key_sorted = airlines_keys_values_reduce.sortByKey()
        
    return(airlines_key_sorted)
    


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """

    flight_cancelled = flights.select('Airline', 'Cancelled','Month')
    
    cancelled_flights = flights.filter((flights.Cancelled == True) & (flights.Month == 9))
    
    flight_cancelled = cancelled_flights.select('Airline', 'Cancelled','Month')
    
    flights_withcount= flight_cancelled.groupBy('Airline').count()   
    
    flight_with_max_cancelled = flights_withcount.orderBy(col("count").desc()).first()
    
    return flight_with_max_cancelled.Airline


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    
    dates = ('2021-11-20', '2021-11-30')

    diverted_flights_filter = flights.filter(flights.FlightDate.between(*dates) & flights.Diverted == True)

    diverted_flights = diverted_flights_filter.select('Airline', 'Diverted')

    return diverted_flights.count()

def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to
    Chicago, IL.
    """
    nashville_chicago_flights_filter = flights.filter((flights.OriginCityName == 'Nashville, TN') & (flights.DestCityName == 'Chicago, IL'))
    
    nashville_chicago_flights = nashville_chicago_flights_filter.select('OriginCityName', 'DestCityName','AirTime')

    nashville_avg_time = nashville_chicago_flights_filter.agg({'AirTime': 'avg'}).collect()[0]['avg(AirTime)']

    return nashville_avg_time


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing.
    """

    flights_nullDeptime = flights.filter(flights.DepTime.isNull())
        
    flights_to_process = flights_nullDeptime.select('Airline','FlightDate','DepTime')
    
    return flights_to_process.select('FlightDate').distinct().count()


def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext(spark_context)
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