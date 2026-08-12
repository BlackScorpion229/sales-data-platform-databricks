"""Generic SCD Type 2 upsert engine (mirrors notebook 04).

Change detection by hashing the tracked business attributes; closure at the
source's `updated_date`; idempotent (a no-op re-run is a no-op).

Requires PySpark at call time only — the module itself is importable
anywhere (e.g., for local unit tests of the pure helpers).
"""

from __future__ import annotations


def _hash_cols(df, attr_cols):
    from pyspark.sql import functions as F  # noqa: PLC0415

    return df.withColumn(
        "_attr_hash",
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in attr_cols]), 256),
    )


def scd2_upsert(spark, target_table: str, source_df, key_col: str,
                attr_cols: list[str], start_col: str, version_col: str = "updated_date") -> dict:
    """Type-2 upsert of a master dimension. Returns a summary dict.

    Args:
        spark: SparkSession
        target_table: qualified Delta table name (e.g. "gold.dim_customer")
        source_df: latest master state (from Silver)
        key_col: natural key (e.g. "customer_id")
        attr_cols: business attributes tracked for change detection
        start_col: source column for effective_start_date of new versions
        version_col: source column giving the moment an attribute changed
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    source = _hash_cols(source_df, attr_cols)
    if not spark.catalog.tableExists(target_table):
        dim = (
            source.drop("_attr_hash")
            .withColumn("effective_start_date", F.col(start_col))
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        dim.write.saveAsTable(target_table)
        return {"closed": 0, "opened": dim.count(), "total": dim.count(), "current": dim.count()}

    tgt = spark.read.table(target_table)
    if "_attr_hash" in tgt.columns:
        tgt = tgt.drop("_attr_hash")
    current = _hash_cols(tgt.filter("is_current = true"), attr_cols)

    non_key = [c for c in source.columns if c not in (key_col, "_attr_hash")]
    src = source.select(
        *[F.col(c).alias(f"s_{c}") for c in non_key],
        F.col(key_col),
        F.col("_attr_hash").alias("src_hash"),
    )
    cur = current.select(
        *[F.col(c).alias(f"t_{c}") for c in current.columns if c not in (key_col, "_attr_hash")],
        F.col(key_col),
        F.col("_attr_hash").alias("tgt_hash"),
    )

    changes = cur.join(src, on=key_col, how="inner").filter(F.col("tgt_hash") != F.col("src_hash"))

    closes = changes.select(
        F.col(key_col).alias("_key_to_close"),
        F.coalesce(F.col(f"s_{version_col}"), F.current_date()).alias("_close_date"),
    )

    new_src = src.withColumnRenamed("src_hash", "_attr_hash")
    for c in list(new_src.columns):
        if c.startswith("s_"):
            new_src = new_src.withColumnRenamed(c, c[2:])

    changed_ids = changes.select(key_col)
    new_versions = (
        new_src.join(cur.select(key_col), on=key_col, how="left_anti")   # brand-new
        .union(new_src.join(changed_ids, on=key_col, how="inner"))       # changed
        .withColumn("effective_start_date", F.coalesce(F.col(version_col), F.col(start_col), F.current_date()))
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .drop("_attr_hash")
    )

    closes.createOrReplaceTempView("closes_v")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING closes_v c
        ON t.{key_col} = c._key_to_close AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            effective_end_date = c._close_date,
            is_current         = false
        """
    )
    new_versions.write.mode("append").saveAsTable(target_table)

    after = spark.read.table(target_table)
    n_cur = after.filter("is_current = true").count()
    return {"closed": closes.count(), "opened": new_versions.count(),
            "total": after.count(), "current": n_cur}
