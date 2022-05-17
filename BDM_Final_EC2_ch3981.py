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

def filter_cbg(input,cbg_list):
    output = []
    for item in input:
        if item == {}: output.append('')
        else:
            out = []
            for i in item:
                if i in cbg_list: out.append((i,item[i]))
            if out != []: output.append(out)
            else: output.append('')
    return output

def transform_cbg(input,cbg_list):
    t = Transformer.from_crs(4326, 2263)
    if type(input) == list: 
        output = []
        for item in input:
            if item == '': output.append('')
            else:
                out = []
                for i in item:
                    for c in cbg_list:
                        if i[0] == c[0]: out.append((t.transform(c[1],c[2]),i[1]))
                output.append(out)
        return output
    else:
        for c in cbg_list:
            if input == c[0]: return t.transform(c[1],c[2])

def distance(start_list,end):
    output = []
    for item in start_list:
        if item == '': output.append('')
        else:
            distance_list=[]
            for start in item:
                distance_list.append((Point(start[0][0],start[0][1]).distance(Point(end[0],end[1]))/5280,start[1]))
            output.append(distance_list)
    return output

def mean(input):
    output = []
    for item in input:
        if item == '': output.append('')
        else:
            sum = 0
            count = 0
            for i in item:
                sum += i[0] * i[1]
                count += i[1]
            if count != 0:
                output.append(str(round(sum/count,2)))
    return output

def median(input):
    output = []
    for item in input:
        if item == '': output.append('')
        else:
            ls = []
            for i in item: ls.append(i[0] * i[1])
            ls_len = len(ls)
            ls.sort()
            index = (ls_len - 1)//2
            if (ls_len % 2): output.append(str(round(ls[index],2)))
            else: output.append(str(round((ls[index]+ls[index+1])/2,2)))
    return output
            
if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)

# read    
    pattern = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020').map(lambda x: next(csv.reader([x])))
    header = pattern.first()
    pattern = pattern.filter(lambda row : row != header) 
    pattern_clean = pattern.map(lambda x: [x[0], '-'.join(x[12].split('T')[0].split('-')[:2]), '-'.join(x[13].split('T')[0].split('-')[:2]), x[18], json.loads(x[19])])

    core = sc.textFile('/tmp/bdm/core-places-nyc.csv')    
    header2 = core.first()
    core = core.filter(lambda row : row != header2) 
    filter_list = core.map(lambda x: [x[0], x[9]])
    filter_list = filter_list.filter(lambda x: x[1].startswith('4451')).collect()
    pat = pattern_clean.filter(lambda x: x[0] in filter_list)

    cbg = sc.textFile('nyc_cbg_centroids.csv')
    header3 = cbg.first()
    cbg = cbg.filter(lambda row : row != header3) 

# extract date
    pat_date = pat.map( lambda x: (x[3],to_datelist(x[1],x[2],x[4]))).filter(lambda x: x[1] is not None).reduceByKey(lambda x,y: merge_datelist(x,y))

# filter nyc cbg
    cbg_filter = cbg.map(lambda x: x.split(',')[0]).collect()
    pat_date_cen = pat_date.map(lambda x: [x[0],filter_cbg(x[1],cbg_filter)])

# transform cbg centroid coordinates
    rdd_cbg_list = cbg.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2]]).collect()
    pat_date_cen = pat_date_cen.map(lambda x: [x[0],transform_cbg(x[0],rdd_cbg_list),transform_cbg(x[1],rdd_cbg_list)])

# mean distance
    pat_date_dis = pat_date_cen.map(lambda x: [x[0],distance(x[2],x[1])])
    pat_date_disM = pat_date_dis.map(lambda x: [x[0],mean(x[1])])

# final output
    output = pat_date_disM.map(lambda x: [str(x[0]),str(x[1][0]),str(x[1][1]) ,str(x[1][2]),str(x[1][3])])\
            .toDF(['cbg_fips', '2019-03' , '2019-10' , '2020-03' , '2020-10'])\
            .sort('cbg_fips', ascending = True)

    output.coalesce(1).write.options(header='true').csv(sys.argv[1])
