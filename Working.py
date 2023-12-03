import os
from itertools import permutations
from dotenv import load_dotenv
from pyspark import RDD, SparkContext
from collections import Counter
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import sum, col, desc


# from operator import add

### install dependency ###
# pip install python-dotenv
# pip install pyspark # make sure you have jdk installed
#####################################

### please update your relative path while running your code ###
temp_airline_textfile = r'C:\Code\DSD\Assignment4_DSD_Spark\flights_data.txt'
temp_airline_csvfile = r"C:\Code\DSD\Assignment4_DSD_Spark\Combined_Flights_2021.csv"
# temp_airline_csvfile = r"C:\Code\DSD\Assignment4_DSD_Spark\TestFlights.csv"


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
    
#     print("1")
#     print(flights_mapped.collect()) #one
    
    
    
    
    flight_pairs = flights_mapped.map(lambda flight: ((flight[0], flight[2]), [ flight[1] ]))
    
#     print("2")
#     print(flight_pairs.collect()) #two
    
# #     flight_pairs_reducer = flight_pairs.map(lambda x: [((tupple[0],tupple[1],tupple[2]), 1) for tupple in x])

    
# #     print("2.1")
# #     print(flight_pairs_reducer.collect())


    
    
    
    
#     flight_pair_list=flight_pairs.groupByKey().mapValues(list).collect()
    
#     print("3")
#     print(flight_pair_list)
    
    
# #     flight_pair_count_airlines = flight_pairs.groupByKey().countByValue().items()
    
    
# #     print("4")
# #     print(flight_pair_count_airlines)
    
    def countLen(x):
        final_dict= Counter(x[1])
        #return (x[0],list(final_dict.items())) # remove x[0] if you dont need
        
        #return (x[0],final_dict.most_common())
        
        return (x[0], Counter(x[1])) # remove x[0] if you dont need

        
        
    
  

 
 # Group flights by date and origin
# grouped_flights = flight_pairs.groupByKey()
#     flight_origin = flights_mapped.reduceByKey(lambda x, y: x+y)
    
#     print(flight_origin.collect())

    flight_reduced = flight_pairs.reduceByKey(lambda a, b: a + b)
    
    flight_reduced_count = flight_reduced.map(countLen)
    
    #flight_reduced_count =  flight_reduced.countByValue().items()


#     print("4")
#     print(flight_reduced.collect()) #threee
    
#     print("5")
#     print(flight_reduced_count.collect()) #four
    
#     print("5")
#     flight_reduced_count = flight_reduced.map(countLen)

    
   
    
    
#     print("6")
        
    flight_filter_count = flight_reduced_count.filter(lambda x : len(x[1]) >1) ## filter rows with just one airline
    
  
# print(flight_filter_count.collect()) #four
    
    
    def getPairs(x):
        combination = [] # empty list 
        
        final_pairs = {} #empty dict
        
        combination.extend(permutations(x[1].keys(), 2))
        
        
        
        for index, element in enumerate(combination):
            
            if (element[1],element[0]) in final_pairs.keys():
                continue
            final_pairs[element] = min(x[1][element[0]],x[1][element[1]])
            
       
        return final_pairs

    
#     print("7")
    flight_filtered_pairs = flight_filter_count.map(getPairs)
    
#     print(flight_filtered_pairs.collect()) #five
    
    
#     print("8")
    
    airlines_keys_values = flight_filtered_pairs.flatMap(lambda x : [(k, v) for k, v in x.items()])
    
    

#     print(airlines_keys_values.collect()) #six
    
    
#     print("9")
    
#     print(airlines_keys_values.groupByKey().collect())
    
#     print(airlines_keys_values.values().collect())
    
#     airlines_keys_values_reduce = airlines_keys_values.reduceByKey(lambda x,y : x+y)
    
    
    print("final")
    
    airlines_keys_values_reduce = airlines_keys_values.reduceByKey(lambda x,y : x+y)
    
#     print(airlines_keys_values_reduce.collect()) #seven

    
    
#     print("final")
    
#    print(airlines_keys_values.collect())

#     print(airlines_keys_values.reduceByKey(add).collect())

    return(airlines_keys_values_reduce)
    





    
    

                                  
                                  
                                              


    


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    
#     flights.printSchema()
   
#     print('Number of rows in dataset:', flights.count())
    flight_cancelled = flights.select('Airline', 'Cancelled','Month')
    
#     flight_cancelled.show()

    cancelled_flights = flights.filter((flights.Cancelled == True) & (flights.Month == 9))
    flight_cancelled = cancelled_flights.select('Airline', 'Cancelled','Month')

#     print(flight_cancelled.show())
    
#     print(flight_cancelled.count())
#     titanic.agg({'Age': 'min'})
#     flights_withcount= flight_cancelled.groupBy('Airline').count().orderBy('count')
    
    flights_withcount= flight_cancelled.groupBy('Airline').count()
    
    
    flight_with_max_cancelled = flights_withcount.orderBy(col("count").desc()).first()
    
#     print("cancelled flight")
    
#     print(flight_with_max_cancelled.Airline)
    
#     print(calc.show())
   
    
#     sort(desc("count_cancelled"))
    
#     flights_data = flights_withcount.collect()
    
#     print(flights_data)
    
#     print(flights_data[0].Airline)

    
    
#     print(flight_cancelled.show())


    return flight_with_max_cancelled.Airline


#     raise NotImplementedError("Your Implementation Here.")


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
#     flights.printSchema()
    
    dates = ('2021-11-20', '2021-11-30')

    #filter DataFrame to only show rows between start and end dates
    diverted_flights_filter = flights.filter(flights.FlightDate.between(*dates) & flights.Diverted == True)

    diverted_flights = diverted_flights_filter.select('Airline', 'Diverted')

#     print(diverted_flights.show())
    return diverted_flights.count()

def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to
    Chicago, IL.
    """
#     flights.printSchema()
    nashville_chicago_flights_filter = flights.filter((flights.OriginCityName == 'Nashville, TN') & (flights.DestCityName == 'Chicago, IL'))

    nashville_chicago_flights = nashville_chicago_flights_filter.select('OriginCityName', 'DestCityName','ActualElapsedTime')

#     nashville_chicago_flights.show()
    nashville_avg_time = nashville_chicago_flights_filter.agg({'ActualElapsedTime': 'avg'}).collect()[0]['avg(ActualElapsedTime)']

#     nashville_chicago_flights_filter.avg('ActualElapsedTime').show()

#     avg_time_df.collect()[0]['avg_time']

#     print(nashville_avg_time)
    
    return nashville_avg_time

    
    
#     raise NotImplementedError("Your Implementation Here.")


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing.
    """
#     raise NotImplementedError("Your Implementation Here.")


    flights_nullDeptime = flights.filter(flights.DepTime.isNull())
    
    
    flights_to_process = flights_nullDeptime.select('Airline','FlightDate','DepTime')
    
#     print(flights_to_process.select('FlightDate').distinct().count())
    
    return flights_to_process.select('FlightDate').distinct().count()
    
#     print(flights_to_process.show())


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