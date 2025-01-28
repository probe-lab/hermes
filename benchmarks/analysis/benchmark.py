import argparse
import json
import polars as pl
import seaborn as sns

PARQUET_BENCHMARK = "parquet"
S3_BENCHMARK = "s3"


class S3Benchmark():

    def __init__(self, json_file: str):
        self._df = self._df_from_json(json_file)

    def _df_from_json(self, file: str) -> pl.DataFrame:
        df = pl.DataFrame({
            "batcher_size": [],
            "traces": [],
            "raw(MB)": [],
            "parquet(MB)": [],
            "submission_time(ms)": [],
            "upload_speed(MB/s)": [],
        })
        cnt = 0
        with open(file, "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break  # EOF
                if line.__contains__('"msg":""') or \
                        line.__contains__('"msg":"running benchmarks"') or \
                        line.__contains__('"msg":"local/dev s3 instance') or \
                        line.__contains__('"msg":"benchmark finished"'):
                    continue  # empty json
                data = json.loads(line)
                new_df = pl.DataFrame({
                    "batcher_size": [data["msg"].split("-")[1]],
                    "traces": [data["traces"]],
                    "raw(MB)": [data["raw(MB)"]],
                    "parquet(MB)": [data["serialized(MB)"]],
                    "submission_time(ms)": [data["parquet-submission-time"]/1_000_000.0],
                    "upload_speed(MB/s)": [data["file-upload-speed(MB/s)"]],
                })
                if cnt == 0:
                    df = new_df
                else:
                    df = pl.concat([df, new_df], how="vertical")
                cnt += 1
        return df

    def plot_metrics(self, output_file: str):
        pass

    def print_metrics(self):
        print("benchmark summary: 's3'")
        summary = self._df.group_by("raw(MB)").agg(
            pl.col("traces").mean(),
            pl.col("submission_time(ms)").mean(),
            pl.col("upload_speed(MB/s)").mean(),
        )
        print(summary)


class ParquetBenchmark():

    def __init__(self, json_file: str):
        self._df = self._df_from_json(json_file)

    def _df_from_json(self, file: str) -> pl.DataFrame:
        df = pl.DataFrame({
            "batcher_size": [],
            "traces": [],
            "raw(MB)": [],
            "parquet(MB)": [],
            "raw_to_parquet_ratio": [],
            "raw_to_parquet_speed(MB/s)": [],
        })
        cnt = 0
        with open(file, "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break  # EOF
                if line.__contains__('"msg":""') or \
                        line.__contains__('"msg":"running benchmarks"') or \
                        line.__contains__('"msg":"benchmark finished"'):
                    continue  # empty json
                data = json.loads(line)
                new_df = pl.DataFrame({
                    "batcher_size": [data["msg"].split("-")[1]],
                    "traces": [data["traces"]],
                    "raw(MB)": [data["raw(MB)"]],
                    "parquet(MB)": [data["serialized(MB)"]],
                    "raw_to_parquet_ratio": [data["raw-to-parquet-ratio"]],
                    "raw_to_parquet_speed(MB/s)": [data["raw-to-parquet-fmt-speed(MB/s)"]],
                })
                if cnt == 0:
                    df = new_df
                else:
                    df = pl.concat([df, new_df], how="vertical")
                cnt += 1
        return df

    def plot_metrics(self, output_file: str):
        pass

    def print_metrics(self):
        print("benchmark summary: 'parquet'")
        summary = self._df.group_by("raw(MB)").agg(
            pl.col("traces").mean(),
            pl.col("raw_to_parquet_ratio").mean(),
            pl.col("raw_to_parquet_speed(MB/s)").mean(),
        )
        print(summary)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        "analyze",
        description="analyze the output of the benchmarks",
        usage="benchmark [file] [options]"
    )
    arg_parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="input file",
    )
    arg_parser.add_argument(
        "--benchmark-type",
        type=str,
        required=True,
        help="the type of the benchmark that we are going to analyze",
    )
    args = arg_parser.parse_args()

    # pase the json into the given benchmark type
    benchmark_type = args.benchmark_type
    if benchmark_type == PARQUET_BENCHMARK:
        # create the Parquetbenchmark
        parquet_benchmark = ParquetBenchmark(args.file)
        parquet_benchmark.print_metrics()
    elif benchmark_type == S3_BENCHMARK:
        s3_benchmark = S3Benchmark(args.file)
        s3_benchmark.print_metrics()
    else:
        print("not valid benchmark type", benchmark_type)
