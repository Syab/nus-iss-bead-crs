from decouple import config
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
from sklearn.cluster import KMeans
import seaborn as sns;

sns.set()
import csv
import boto3

AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')
AWS_REGION = "ap-southeast-1"
AWS_S3_BUCKET = "ebd-demo"


def get_latest_file():
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION)
    my_bucket = s3.Bucket(AWS_S3_BUCKET)
    files = my_bucket.objects.filter(Prefix='input/')
    files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified, reverse=True)][0:1]
    # print(files[0])
    return files[0]


s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION
                         )

AWS_READ_OBJ_KEY = get_latest_file()
response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=AWS_READ_OBJ_KEY)

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    df = pd.read_csv(response.get("Body"))
    df.head(10)

df['p_available'] = df['y'] / df['total_lots'] * 100
df.drop_duplicates(inplace=True)
# print(df.head(20))

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']))
gdf['p_available'] = gdf['p_available'].astype(float)
gdf['p_available'] = gdf['p_available'].fillna(0)

X = gdf.loc[:, ['car_park_no', 'latitude', 'longitude']]
print(X.head(10))

K_clusters = range(1, 10)
kmeans = [KMeans(n_clusters=i) for i in K_clusters]
Y_axis = df[['latitude']]
X_axis = df[['longitude']]
score = [kmeans[i].fit(Y_axis).score(Y_axis) for i in range(len(kmeans))]
# Visualize
plt.plot(K_clusters, score)
plt.xlabel('Number of Clusters')
plt.ylabel('Score')
plt.title('Elbow Curve')

plt.show()

kmeans = KMeans(n_clusters = 8, init ='k-means++')
kmeans.fit(X[X.columns[1:3]]) # Compute k-means clustering.
X['cluster_label'] = kmeans.fit_predict(X[X.columns[1:3]])
centers = kmeans.cluster_centers_ # Coordinates of cluster centers.
labels = kmeans.predict(X[X.columns[1:3]]) # Labels of each point
X.head(20)

figure(figsize=(100, 50))

X.plot.scatter(x = 'latitude',
               y = 'longitude',
               c=labels,
               s=50,
               cmap='icefire')

plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)

plt.show()