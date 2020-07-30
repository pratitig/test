
import boto3


import fastparquet as fp
s3 = s3fs.S3FileSystem()
fs = s3fs.core.S3FileSystem()

client = boto3.client('s3')


def describe_numeric_1d(series: pd.Series) -> dict:
    """Describe a numeric series.
    Args:
        series: The Series to describe.
        series_description: The dict containing the series description so far.
    Returns:
        A dict containing calculated series description values.
    Notes:
        When 'bins_type' is set to 'bayesian_blocks', astropy.stats.bayesian_blocks is used to determine the number of
        bins. Read the docs:
        https://docs.astropy.org/en/stable/visualization/histogram.html
        https://docs.astropy.org/en/stable/api/astropy.stats.bayesian_blocks.html
        This method might print warnings, which we suppress.
        https://github.com/astropy/astropy/issues/4927
         quantiles:
                - 0.05
                - 0.25
                - 0.5
                - 0.75
                - 0.95
    """
    # number of observations in the Series
    leng = len(series)
    # TODO: fix infinite logic
    # number of non-NaN observations in the Series
    count = series.count()
    # number of infinite observations in the Series
    n_infinite = count - series.count()
    quantiles = [0.05,0.25,0.5,0.75,0.95]
    # TODO: check if we prefer without nan
    distinct_count = series.nunique()

    stats = {
        "mean": series.mean(),
        "std": series.std(),
        "is_unique": distinct_count == leng,
        "mode": series.mode().iloc[0] if count > distinct_count > 1 else series[0],
        "p_unique": distinct_count * 1.0 / leng,
        "memorysize": series.memory_usage(),
    }

    stats["range"] = stats["max"] - stats["min"]
    stats.update(
        {
            "{:.0%}".format(percentile): value
            for percentile, value in series.quantile(quantiles).to_dict().items()
        }
    )
    stats["iqr"] = stats["75%"] - stats["25%"]
#     stats["cv"] = stats["std"] / stats["mean"] if stats["mean"] else np.NaN
    stats["p_zeros"] = float(stats["n_zeros"]) / len(series)
#     bins = 50
#     stats["histogram_bins"] = bins
    return stats

print("Data Profiling for all files are completed.")
