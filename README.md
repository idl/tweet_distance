#Tweet Distance
Calculates distances of geo-coordinates [lat,long] of tweets from shapes present in a road-map shape file. The process is parallelized using python multiprocessing library. 

## Requirements
1. [GDAL](https://pypi.python.org/pypi/GDAL/)
2. Multiprocessing

## Utilization

```
python apply_distance.py <input_csv_file> <output_csv_file>
```

Input file format (input_csv_file):

ID  | Geo
--- | ---
1234  | [Lat,Long]
