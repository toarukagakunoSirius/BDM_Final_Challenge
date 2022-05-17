import csv
import json
import sys
import pyspark
from pyspark.sql import SparkSession

from pyproj import Transformer
from shapely.geometry import Point

def to_datelist(start,end,cbg):
    if start =='2019-03' or end == '2019-03': return [cbg,{},{},{}]
    elif start =='2019-10' or end == '2019-10': return [{},cbg,{},{}]
    elif start =='2020-03' or end == '2020-03': return [{},{},cbg,{}]
    elif start =='2020-10' or end == '2020-10': return [{},{},{},cbg]
    else: None

def merge_datelist(x,y):
    output = [{},{},{},{}]
    for i in range(len(x)):
        output[i].update(x[i])
        output[i].update(y[i])
    return output

def filter_cbg(input,centroids):
    output = []
    for dict_ in input:
        if dict_ == {}: output.append('')
        else:
            dict_out = []
            for item in dict_:
                if item in centroids:
                    dict_out.append((item,dict_[item]))
            if dict_out != []:  
                output.append(dict_out)
            else:
                output.append('')
    return output

def transform_cbg(input,centroids):
    t = Transformer.from_crs(4326, 2263)
    if type(input) == list: 
        list_out = []
        for dict_ in input:
            if dict_ == '': list_out.append('')
            else:
                dict_out = []
                for item1 in dict_:
                    for item2 in centroids:
                        if item1[0] == item2[0]:
                            dict_out.append((t.transform(item2[1],item2[2]),item1[1]))
                list_out.append(dict_out)
        return list_out
    else:
        for item in centroids:
            if input == item[0]:
                return t.transform(item[1],item[2])

def distance(start_list,end):
    output = []
    for item in start_list:
        if item == '': output.append('')
        else:
            dist=[]
            for start in item:
                dist.append((Point(start[0][0],start[0][1]).distance(Point(end[0],end[1]))/5280,start[1]))
            output.append(dist)
    return output

def mean(input):
    output = []
    for item in input:
        if item == '':
            output.append('')
        else:
            sum_ = 0
            num_ = 0
            for cuple in item:
                sum_ += cuple[0] * cuple[1]
                num_ += cuple[1]
            if num_ != 0:
                output.append(str(round(sum_/num_,2)))
    return output

if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)

# read
    pattern = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020').map(lambda x: next(csv.reader([x])))
    header = pattern.first()
    pattern = pattern.filter(lambda row : row != header)
    pattern_clean = pattern.map(lambda x: [x[0], '-'.join(x[12].split('T')[0].split('-')[:2]), '-'.join(x[13].split('T')[0].split('-')[:2]), x[18], json.loads(x[19])])

    markets = sc.textFile('nyc_supermarkets.csv')
    filter_list = markets.map(lambda x: x.split(',')[-2]).collect()
    pattern_clean = pattern_clean.filter(lambda x: x[0] in filter_list)

    centroids = sc.textFile('nyc_cbg_centroids.csv')
    header = centroids.first()
    centroids = centroids.filter(lambda row : row != header) 

# extract date
    pat_date = pattern_clean.map( lambda x: (x[3],to_datelist(x[1],x[2],x[4]))).filter(lambda x: x[1] is not None).reduceByKey(lambda x,y: merge_datelist(x,y))

# filter nyc cbg
    centroids_filter = centroids.map(lambda x: x.split(',')[0]).collect()
    pat_date_cen = pat_date.map(lambda x: [x[0],filter_cbg(x[1],centroids_filter)])

# transform cbg centroid coordinates
    centroids_list = centroids.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2]]).collect()
    pat_date_cen = pat_date_cen.map(lambda x: [x[0],transform_cbg(x[0],centroids_list),transform_cbg(x[1],centroids_list)])

# mean distance
    pat_date_dis = pat_date_cen.map(lambda x: [x[0],distance(x[2],x[1])])
    pat_date_disM = pat_date_dis.map(lambda x: [x[0],mean(x[1])])

# final output
    output = pat_date_disM.map(lambda x: [str(x[0]),str(x[1][0]),str(x[1][1]) ,str(x[1][2]),str(x[1][3])])\
            .toDF(['cbg_fips','2019-03','2019-10','2020-03','2020-10'])\
            .sort('cbg_fips', ascending = True)

    output.coalesce(1).write.options(header='true').csv(sys.argv[1])