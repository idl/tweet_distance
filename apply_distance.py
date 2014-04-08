from multiprocessing import Process, Queue, Pool, cpu_count
from osgeo import ogr
import json
import csv
import ast
import time
import sys
from operator import itemgetter

# #globals, bad but effective. Used for async
# number_of_tweets = 0
# number_with_geo = 0
# responses =[]

# # cb() used with async
# def cb(result):
#   global responses
#   global number_of_tweets
#   global number_with_geo
#   number_of_tweets += 1
#   if result:
#     responses.append(result)
#     number_with_geo += 1

#   if number_of_tweets % 100 == 0:
#     print "number of tweets: ", number_of_tweets
#     print "number of tweets with low distance:", number_with_geo

def check_distance(shape_f,lon1,lat1):
  driver = ogr.GetDriverByName('ESRI Shapefile')
  try:
    lineshp = driver.Open(shape_f, 0)
    linelyr = lineshp.GetLayer()
  except Exception as e:
    print "got here", e.msg()


  linefeat = linelyr.GetNextFeature()

  point_geom = ogr.Geometry(ogr.wkbPoint)
  point_geom.SetPoint_2D(0, float(lon1),float(lat1))

  distlist = []
  while linefeat:
    # change this according to shapefile definition
    # # major roads
    # st_name = linefeat.GetField("road_name")

    # highways
    st_name = linefeat.GetField('lname')
    
    line_geom = linefeat.GetGeometryRef()
    dist = point_geom.Distance(line_geom) * 111111 
    distlist.append([st_name,dist])
    linefeat.Destroy()
    linefeat = linelyr.GetNextFeature()

  distance = min(distlist, key=lambda x: min(x[1:]))
  return distance

def process_distance(dp_data):
  coordinates = dp_data['coordinates']
  try:
    coordinates = dp_data['coordinates']

    # #Major roads
    # distance = check_distance('./Shape_file_1/atl_major_roads_shp.shp', coordinates[1],coordinates[0])

    #highways
    distance = check_distance('./Shape_file_2/NHPN_STFIPS_13.shp', coordinates[1],coordinates[0])

    
    # distance buffer set at 10. Adjust according to condition
    if distance[1] < 10:
      dp_data['street_name'] = distance[0]
      dp_data['distance'] = distance[1]
      #print dp_data #print to see the matching tweets
      return dp_data
  except:
    pass

def read_input(ifile):
  idata = []
  with open(ifile, 'rU') as f:
    reader = csv.reader(f)
    next(reader, None)
    for row in reader:
      dp_data = {
        'dp_id': row[0],
        'coordinates':ast.literal_eval(row[1]),
        'tweet':row[2],
        'created_at':row[3],
        'user':row[4]
      }
      idata.append(dp_data)
  f.close()
  return idata

def write_to_csv(ofile,odata):
  with open(ofile, 'wb') as csvfile:
    mapwriter = csv.writer(csvfile)

    mapwriter.writerow(['id','longitude','latitude', 'street_name', 'distance','tweet','created_at','user'])

    for each in odata:
      mapwriter.writerow([each['dp_id'],each['coordinates'][1], each['coordinates'][0], each['street_name'], each['distance'],each['tweet'],each['created_at'],each['user']])


def main():
  total = len(sys.argv)

  if total < 3:
    print "Utilization: python apply_distance.py <input_csv_file> <output_csv_file>"
    exit(0)

  pool = Pool(processes=cpu_count())

  idata = read_input(str(sys.argv[1]))

  num_tasks = len(idata)  
  # #async uncomment global and cb() functions
  # for each in idata:
  #   pool.apply_async(process_distance, [each], callback=cb)
  # pool.close()
  # pool.join()
  # print responses

  # #map
  # responses = pool.map(process_distance, idata[:1000])

  #imap
  responses = pool.imap_unordered(process_distance, idata)

  while (True):
    completed = responses._index
    if (completed == num_tasks): break
    percent = (float(completed)/float(num_tasks))*100
    print "%.3f" % percent," % complete. ", "Waiting for", num_tasks-completed, "tasks to complete..."
    time.sleep(2)


  pool.close()

  responses = [x for x in responses if x is not None]

  idata = write_to_csv(str(sys.argv[2]),responses)


if __name__ == "__main__":
  main()
