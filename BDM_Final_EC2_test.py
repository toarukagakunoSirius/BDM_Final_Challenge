from pyspark.sql.session import SparkSession
import sys
import json
from pyspark.sql.functions import col
from pyspark import SparkFiles
import math


def parse_year_month(date_range_start_flag, date_range_end_flag):
    flag = ''
    if (date_range_start_flag == '2019-03' or date_range_end_flag == '2019-03'):
        flag = '2019-03'
    elif (date_range_start_flag == '2019-10' or date_range_end_flag == '2019-10'):
        flag = '2019-10'
    elif (date_range_start_flag == '2020-03' or date_range_end_flag == '2020-03'):
        flag = '2020-03'
    elif (date_range_start_flag == '2020-10' or date_range_end_flag == '2020-10'):
        flag = '2020-10'

    return flag


# haversine distance unit is km
def haversine2(lonlat1, lonlat2):
    lat1,lon1 = lonlat1
    lat2,lon2 = lonlat2

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = math.sin(d_lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon * 0.5) ** 2
    a = 2 * math.asin(math.sqrt(a)) * 6371
    return a


def parse_vistor_home_cbgs(vistors_home_cbgs, list_nyc_cbg_centroids):
    dict_cbgs = json.loads(vistors_home_cbgs)
    cbgs = list(dict_cbgs.keys())
    if (len(cbgs) == 0):
        return None
    # only consider CBG FIPS for NYC
    cbgs = list(filter(lambda x: x in list_nyc_cbg_centroids, cbgs))
    if(len(cbgs) == 0):
        cbgs = None
    return cbgs


#return list of distance,then calculate median of distance
def distances(x,dict_cbg_latlong):
    poi_cbg_latlong = dict_cbg_latlong.get(x[0][0])
    distance_list = list(map(lambda cbg:haversine2(dict_cbg_latlong.get(cbg),poi_cbg_latlong),x[1]))
    return (x[0],distance_list)

#calculate median of a list
def median(list):
    result = []
    for i in list:
      result.extend(i)
    result.sort()

    if (len(result) == 1):
        median = result[0]
    elif (len(result) % 2 == 1):
        median = result[int(len(result) / 2)]
    else:
        index = int(len(result) / 2)
        median = (result[index] + result[index - 1]) / 2
    return median


def format_month_distance(x):
    cbg_fips = int(x[0])
    date_distances = dict(x[1])
    return (cbg_fips, date_distances.get('2019-03', ''), date_distances.get('2019-10', ''),date_distances.get('2020-03', ''), date_distances.get('2020-10', ''))


if __name__ == '__main__':
    output = sys.argv[1]
    import os
    nyc_cbg_centroids = os.getenv('SPARK_YARN_STAGING_DIR') + "/nyc_cbg_centroids.csv"
    core_places_nyc = os.getenv('SPARK_YARN_STAGING_DIR') + "/core-places-nyc.csv"
    weekly_patterns = "/tmp/bdm/weekly-patterns-nyc-2019-2020/*"
    import time
    start_time = time.time()
    spark = SparkSession.builder.appName("BDM_Final_EC2_gj2065").getOrCreate()

    #"placekey", "safegraph_place_id", "parent_placekey", "parent_safegraph_place_id", "location_name",
    #"safegraph_brand_ids", "brands", "top_category", "sub_category", "naics_code", "latitude", "longitude",
    #"street_address", "city", "region", "postal_code", "iso_country_code", "phone_number", "open_hours",
    #"category_tags", "opened_on", "closed_on", "tracking_opened_since", "tracking_closed_since"
    core_places_nyc_data = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', False) \
        .csv(core_places_nyc) \
        .select('placekey', 'naics_code')\
        .cache()

    weekly_patterns_data = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', False) \
        .option('escape', '"') \
        .csv(weekly_patterns) \
        .select('placekey', 'poi_cbg', 'visitor_home_cbgs', 'date_range_start', 'date_range_end') \
        .withColumn('date_range_start_year_month', col('date_range_start').substr(0, 7)) \
        .withColumn('date_range_end_year_month', col('date_range_end').substr(0, 7)) \
        .cache()

    #'cbg_fips', 'latitude', 'longitude'
    nyc_cbg_centroids_data = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', True) \
        .csv(nyc_cbg_centroids) \
        .cache()

    # 1.match all placekeys with naics_code starts with 4451 in the core-places-nyc.csv
    weekly_patterns_data = weekly_patterns_data.join(core_places_nyc_data, 'placekey') \
        .filter(col('naics_code').startswith('4451'))

    # 2. Only visit patterns with date_range_start or date_range_end overlaps with the 4
    # months of interests (Mar 2019, Oct 2019, Mar 2020, Oct 2020) will be considered,
    # i.e. either the start or the end date falls within the period.
    weekly_patterns_data = weekly_patterns_data.rdd.map(lambda row: [row['poi_cbg'], row['visitor_home_cbgs'],parse_year_month(row['date_range_start_year_month'],row['date_range_end_year_month'])]) \
        .filter(lambda x: len(x[2]) > 0)

    # 3. Use visitor_home_cbgs as the travel origins, and only consider CBG FIPS for NYC (must exist in nyc_cbg_centroids.csv ).
    list_nyc_cbg_centroids = nyc_cbg_centroids_data.select('cbg_fips') \
        .rdd \
        .map(lambda row: str(row[0])) \
        .collect()

    weekly_patterns_data = weekly_patterns_data.map(lambda row: ((row[0], row[2]), parse_vistor_home_cbgs(row[1], list_nyc_cbg_centroids))) \
        .filter(lambda x: x[1] != None)


    '''4. Travel distances are the distances between each CBG in visitor_home_cbgs with
    poi_cbg . The distance must be computed in miles. To compute the distance
    between coordinates locally to NYC more accurately, please project them to the
    EPSG 2263 first.
    '''
    map_cbg_lat_lon = nyc_cbg_centroids_data.rdd \
        .map(lambda row: (str(row['cbg_fips']), [row['latitude'], row['longitude']])) \
        .collectAsMap()

    result = weekly_patterns_data.map(lambda x: distances(x, map_cbg_lat_lon)) \
        .groupByKey() \
        .mapValues(lambda values: round(median(values) * 0.621371, 2)) \
        .map(lambda x: (x[0][0], [x[0][1], x[1]]))\
        .groupByKey() \
        .map(lambda group: format_month_distance(group)) \
        .repartition(1) \
        .sortBy(lambda x: x[0]) \
        .saveAsTextFile(output)

    end_time = time.time()
    print('finished,time costs %s s' % ((end_time-start_time)))
