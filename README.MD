## Da trang xe cat bien dong 

You have a bunch of data inside your lovely Postgres, but its design does not allow efficient aggregated queries?
You then end up acquire another database just for this usecase (AlloyDB, Clickhhouse...), or in my old company, we threw everything to Bigquery.

I start this project for fun, but with a question in mind: Can i provide a self-hosted solution but with significant enough cost saving. 

Tech stacks I will use:
- Postgres and logical replication (I just want to see how it works), the same principle can applied to Mysql Binlog, Mongodb Oplog, or more generally Debezium
- Rust (I cannot apply for Rust jobs because people don't believe I can efficiently write Rust &#128546; )
- Datafusion (A big fan, also a project I am contributing to)

This looks somewhat similar to the combo of Debezium: https://debezium.io/blog/2021/10/20/using-debezium-create-data-lake-with-apache-iceberg/

## High level diagram

### Ingestion flow

```
Postgres -> logical replication -> Ingestor -> Object store (Parquet) 
                                        |
                                        --> Secondary index (Postgres)
```

### Query flow
```
Query -> Datafusion framework -> Custom Postgres TableProvider -> Object store (Parquet)
                                            |
                                            v
                                    Secondary index (Postgres)
```

### Schema for secondary index

I'll try to keep this project simple at first. For different queries exist different ways/types of metadata we need to maintain inside secondary index, I will put basic items here to remind myself (It won't cover everything though)

- [ ] Rowgroup metadata 
- [ ] Bloomfilter 
- [ ] PageIndex metadata (For finer page skipping)
- [ ] Fulltext search???


We need a schema for this index

| file_location string| row_group_offset int| statistics (json) | bloomfilter | page_index_metadata |

The purpose is for learning here, so i'll try to use json where possible (although it is not as optimal storage format)


An example of row_group_statistics
```json
{
    "num_row":10000,
    "column_statistics": [
        {
            "null_count": 10,
            "max": binary,
            "min": binary,
            "sum": binary,
            "distinct_count": 10
        }
    ]
}
```

With this metadata, we can apply skip scanning at `row_group` level, or even the whole parquet file


## Reference
https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/
