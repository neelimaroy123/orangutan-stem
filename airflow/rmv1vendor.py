import json
import json_stream
import json_stream.httpx
from json_stream.dump import JSONStreamEncoder
from rmapi import get_rmv1_client
from redshift import get_redshift, etl_post_status
import smart_open

if __name__ == "__main__":
    with get_redshift() as conn, conn.cursor() as cur, get_rmv1_client() as client:
        with client.stream(
            method="GET",
            url=f"api/Vendors",
            params={
                "ModifiedSince": "2020-01-01",
                "IncludeRelatedData": "false",
            },
        ) as r, smart_open.open(
            "s3://rm-api-v1/Vendors.jsonl.gz", "w", errors="replace"
        ) as out:
            data = json_stream.httpx.load(r)
            rowcount = 0
            for result in data.persistent():
                json.dump(result, out, cls=JSONStreamEncoder, ensure_ascii=False)
                out.write("\n")
                rowcount += 1
            print(f"Downloaded {rowcount} rows")
        if rowcount < 12000:
            raise RuntimeError(f"Downloaded only {rowcount} rows for Vednors")
        cur.execute("truncate table stgbmscat.rm_vendor_v1_lnd")
        cur.execute("""
  copy stgbmscat.rm_vendor_v1_lnd
    from 's3://rm-api-v1/Vendors.jsonl.gz'
    iam_role 'arn:aws:iam::079672631794:role/AWSRedshift-S3CopyRole'
    json 'auto ignorecase'
    gzip
    dateformat 'auto'
    timeformat 'auto'
                      """)
        conn.commit()
    etl_post_status("stgbmscat", "rm_vendor_v1_lnd", "success")
