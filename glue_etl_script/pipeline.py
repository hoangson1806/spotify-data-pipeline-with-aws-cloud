import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Tracks
Tracks_node1710944370825 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-bucket-hoangson/staging-layer/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="Tracks_node1710944370825")

# Script generated for node Artist
Artist_node1710944370593 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-bucket-hoangson/staging-layer/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="Artist_node1710944370593")

# Script generated for node Album
Album_node1710944370377 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-bucket-hoangson/staging-layer/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="Album_node1710944370377")

# Script generated for node Join album-artist
Joinalbumartist_node1710945672488 = Join.apply(frame1=Album_node1710944370377, frame2=Artist_node1710944370593, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumartist_node1710945672488")

# Script generated for node Join
Join_node1710945977065 = Join.apply(frame1=Joinalbumartist_node1710945672488, frame2=Tracks_node1710944370825, keys1=["track_id"], keys2=["id"], transformation_ctx="Join_node1710945977065")

# Script generated for node Drop Fields
DropFields_node1710946076737 = DropFields.apply(frame=Join_node1710945977065, paths=["`.id`"], transformation_ctx="DropFields_node1710946076737")

# Script generated for node Destination
Destination_node1710946141324 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1710946076737, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-bucket-hoangson/data-warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1710946141324")

job.commit()