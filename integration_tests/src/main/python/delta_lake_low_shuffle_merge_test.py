# Copyright (c) 2024-2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyspark.sql.functions as f
import pytest

from conftest import is_databricks_runtime
from delta_lake_merge_common import *
from marks import *
from pyspark.sql.types import *
from spark_session import is_databricks173_or_later, spark_version

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true",
                            "spark.rapids.sql.delta.lowShuffleMerge.enabled": "true"})

# The Delta 2.4 implementation still uses a per-file iterator. The DBR 17.3 implementation
# tracks file boundaries for every reader mode and deliberately does not force PERFILE.
if not is_databricks173_or_later():
    delta_merge_enabled_conf = copy_and_update(
        delta_merge_enabled_conf,
        {"spark.rapids.sql.format.parquet.reader.type": "PERFILE"})
else:
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    delta_merge_enabled_conf = copy_and_update(
        delta_merge_enabled_conf,
        {"spark.sql.adaptive.enabled": "false"})


def supports_delta_low_shuffle_merge():
    return is_databricks173_or_later() or \
        (not is_databricks_runtime() and spark_version().startswith("3.4"))


def assert_low_shuffle_merge(do_merge, data_path, conf, expect_write=True):
    assert expect_write
    cpu_result = with_cpu_session(lambda spark: do_merge(spark, data_path + "/CPU"), conf=conf)

    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        gpu_result = with_gpu_session(
            lambda spark: do_merge(spark, data_path + "/GPU"), conf=conf)
        captured_plans = callback.getResultsWithTimeout(10000)
    finally:
        callback.endCapture()

    assert_equal(cpu_result, gpu_result)
    command_name = "GpuLowShuffleMergeCommand"
    assert any(command_name in str(plan) for plan in captured_plans), \
        f"{command_name} was not found in the captured MERGE plans"

@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_low_shuffle_merge_when_gpu_file_scan_override_failed(spark_tmp_path,
                                                                    spark_tmp_table_factory,
                                                                    use_cdf, num_slices):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"

    conf = copy_and_update(delta_merge_enabled_conf,
                           {
                               "spark.rapids.sql.exec.FileSourceScanExec": "false",
                               # Disable auto broadcast join due to this issue:
                               # https://github.com/NVIDIA/spark-rapids/issues/10973
                               "spark.sql.autoBroadcastJoinThreshold": "-1"
                            })
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                   src_table_func, dest_table_func, merge_sql, False, conf=conf)



@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("table_ranges", [(range(20), range(10)),  # partial insert of source
                                          (range(5), range(5)),  # no-op insert
                                          (range(10), range(20, 30))  # full insert of source
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                           use_cdf, partition_columns, num_slices):
    do_test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory,
                                              table_ranges, use_cdf, False, partition_columns,
                                              num_slices, False, delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("table_ranges", [(range(10), range(20)),  # partial delete of target
                                          (range(5), range(5)),  # full delete of target
                                          (range(10), range(20, 30))  # no-op delete
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                       use_cdf, partition_columns, num_slices):
    do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, False, partition_columns, num_slices, False,
                                          delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    do_test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                        num_slices, False, delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="All-reader low shuffle merge is supported on DBR 17.3+")
@pytest.mark.parametrize(
    "reader_type", ["AUTO", "PERFILE", "MULTITHREADED", "COALESCING"], ids=idfn)
def test_databricks_delta_low_shuffle_merge_reader_type(
        spark_tmp_path, spark_tmp_table_factory, reader_type):
    conf = copy_and_update(
        delta_merge_enabled_conf,
        {"spark.rapids.sql.format.parquet.reader.type": reader_type})
    do_test_delta_merge_standard_upsert(
        spark_tmp_path,
        spark_tmp_table_factory,
        use_cdf=False,
        enable_deletion_vectors=False,
        num_slices=10,
        compare_logs=False,
        conf=conf,
        assert_func=assert_low_shuffle_merge)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
@pytest.mark.parametrize("merge_sql", [
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'q' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN NOT MATCHED THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN NOT MATCHED AND s.b > 'q' THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'a' AND s.b < 'g' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN MATCHED AND s.b > 'g' AND s.b < 'z' THEN UPDATE SET d.a = s.a / 4, d.b = concat('extra', s.b)" \
    " WHEN NOT MATCHED AND s.b > 'b' AND s.b < 'f' THEN INSERT *" \
    " WHEN NOT MATCHED AND s.b > 'f' AND s.b < 'z' THEN INSERT (b) VALUES ('not here')" ], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, merge_sql, num_slices):
    do_test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, False, 
                                              merge_sql, num_slices, False, 
                                              delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    do_test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path,
                                                                spark_tmp_table_factory,
                                                                use_cdf,
                                                                False,
                                                                num_slices,
                                                                False,
                                                                delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3+")
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
def test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    do_test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                                delta_merge_enabled_conf)
