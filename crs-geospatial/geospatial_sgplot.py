from decouple import config
import pandas as pd
import geopandas as gpd
import numpy as np
import chart_studio
import plotly.express as px
import plotly.figure_factory as ff
import boto3

chart_studio.tools.set_credentials_file(username=config('PLT_USER'), api_key=config('PLT_APIKEY'))
MAPBOX_ACCESS_TOKEN = config('MAPBOX_TOKEN')

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

df['p_available'] = df['y'] / df['total_lots']
df.drop_duplicates(inplace=True)
# print(df.head(20))

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']))
gdf['p_available'] = gdf['p_available'].astype(float)
gdf['p_available'] = gdf['p_available'].fillna(0)
# print(gdf.head(10))

px.set_mapbox_access_token(MAPBOX_ACCESS_TOKEN)
# fig = px.scatter_mapbox(gdf,
#                         lat=gdf['latitude'],
#                         lon=gdf['longitude'],
#                         hover_name=gdf['car_park_no'],
#                         size=gdf['y'],
#                         color=gdf['y'],
#                         color_continuous_scale=px.colors.cyclical.IceFire,
#                         zoom=12)
# fig.show()

fig2 = ff.create_hexbin_mapbox(
    data_frame=df, lat="latitude", lon="longitude",
    nx_hexagon=80, opacity=0.5,
    labels={"color": "Percentage Availability"},
    color='p_available',
    min_count=1,
    color_continuous_scale="icefire",
    # show_original_data=True,
    # original_data_marker=dict(size=5, opacity=0.6, color="orange"),
    zoom=12
)
fig2.show()

fig3 = ff.create_hexbin_mapbox(
    data_frame=df, lat="latitude", lon="longitude",
    nx_hexagon=80, opacity=0.5,
    labels={"color": "Percentage Availability"},
    color='p_available',
    color_continuous_scale="Viridis",
    min_count=1,
    zoom=12
)
fig3.show()
