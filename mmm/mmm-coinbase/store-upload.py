#!/usr/bin/python3

from pathlib import Path
import sys
from datetime import datetime, timedelta
import tarfile
import zstandard as zstd
import shutil
import boto3
import io


yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
out_dir = Path(sys.argv[1])
date_dir = out_dir / yesterday

s3 = boto3.client("s3")
cctx = zstd.ZstdCompressor(level=10)

for time_dir in date_dir.iterdir():
    for product_dir in time_dir.iterdir():
        print(product_dir)
        book_dir = product_dir / "book"
        if book_dir.exists():
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                tar.add(book_dir, arcname="book")

            compressed = cctx.compress(buf.getvalue())
            with open(product_dir / "book.tar.zst", "wb") as zipped_book:
                zipped_book.write(compressed)

            shutil.rmtree(book_dir)

        for path in product_dir.iterdir():
            s3.upload_file(str(path), "coinbase-devel", "/".join(path.parts[-4:]))
        shutil.rmtree(product_dir)
    shutil.rmtree(time_dir)
shutil.rmtree(date_dir)
