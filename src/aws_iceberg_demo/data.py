import pathlib

import duckdb


def convert_csv(in_path: pathlib.Path, out_path: pathlib.Path):
    sql = f"COPY '{in_path}' to '{out_path}' (FORMAT 'parquet')"
    duckdb.sql(sql)

def convert_all_csvs(input_folder_path: pathlib.Path, output_folder_path: pathlib.Path):
    in_path: pathlib.Path
    for in_path in input_folder_path.glob("*.csv"):
        out_path = output_folder_path / in_path.with_suffix(".parquet").name
        if out_path.exists():
            continue
        convert_csv(in_path, out_path)


if __name__ == '__main__':
    in_folder = pathlib.Path("/home/anders/projects/tutorials/aws_iceberg_demo/data")
    out_folder = in_folder / "parquet"
    out_folder.mkdir(exist_ok=True)
    convert_all_csvs(in_folder, out_folder)