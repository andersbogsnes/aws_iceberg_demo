from aws_iceberg_demo.connections import get_fs

if __name__ == '__main__':
    fs = get_fs()
    try:
        fs.mkdir("data-engineering-events")
    except FileExistsError:
        pass
