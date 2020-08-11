
import boto3
import fastparquet as fp
s3 = s3fs.S3FileSystem()
#fs = s3fs.core.S3FileSystem()

boto3.client('s3')
resource = boto3.resource('s3')

stats = 
        "mean": series.mean(),
        "std": series.std(),
        "is_unique": distinct_count == leng,
        "mode": series.mode().iloc[0] if count > distinct_count > 1 else series[0],
        "p_unique": distinct_count * 1.0 / leng,
        "memorysize": series.memory_usage(),
        

stats["range"] = stats["max"] - stats["min"]
