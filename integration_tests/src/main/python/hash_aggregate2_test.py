# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import math
import pytest

from asserts import *
from conftest import is_databricks_runtime, spark_jvm
from conftest import is_not_utc
from data_gen import *
from functools import reduce
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import is_databricks104_or_later, with_cpu_session, is_before_spark_330

pytestmark = pytest.mark.nightly_resource_consuming_test

_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
               'spark.rapids.sql.castStringToFloat.enabled': 'true'}

_float_smallbatch_conf = copy_and_update(_float_conf,
        {'spark.rapids.sql.batchSizeBytes' : '250'})

_float_conf_skipagg = copy_and_update(_float_smallbatch_conf,
        {'spark.rapids.sql.agg.skipAggPassReductionRatio': '0'})

_float_conf_partial = copy_and_update(_float_conf,
        {'spark.rapids.sql.hashAgg.replaceMode': 'partial'})

_float_conf_final = copy_and_update(_float_conf,
        {'spark.rapids.sql.hashAgg.replaceMode': 'final'})

# The input lists or schemas that are used by StructGen.

# grouping longs with nulls
_longs_with_nulls = [('a', LongGen()), ('b', IntegerGen()), ('c', LongGen())]
# grouping longs with no nulls
_longs_with_no_nulls = [
    ('a', LongGen(nullable=False)),
    ('b', IntegerGen(nullable=False)),
    ('c', LongGen(nullable=False))]
# grouping longs with nulls present
_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping doubles with nulls present
_grpkey_dbls_with_nulls = [
    ('a', RepeatSeqGen(DoubleGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping floats with nulls present
_grpkey_floats_with_nulls = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping strings with nulls present
_grpkey_strings_with_nulls = [
    ('a', RepeatSeqGen(StringGen(pattern='[0-9]{0,30}'), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping strings with nulls present, and null value
_grpkey_strings_with_extra_nulls = [
    ('a', RepeatSeqGen(StringGen(pattern='[0-9]{0,30}'), length= 20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping single-level structs
_grpkey_structs_with_non_nested_children = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', StringGen(pattern='[0-9]{0,30}')],
        ['ac', DecimalGen()]]), length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping multiple-level structs
_grpkey_nested_structs = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', StringGen(pattern='[0-9]{0,30}')],
        ['ac', StructGen([['aca', LongGen()],
                          ['acb', BooleanGen()],
                          ['acc', StructGen([['acca', StringGen()]])]])]]),
        length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping multiple-level structs with arrays in children
_grpkey_nested_structs_with_array_child = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', ArrayGen(IntegerGen())],
        ['ac', ArrayGen(StructGen([['aca', LongGen()]]))]]),
        length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]

# grouping NullType
_grpkey_nulls = [
    ('a', NullGen()),
    ('b', IntegerGen()),
    ('c', LongGen())]

# grouping floats with other columns containing nans and nulls
_grpkey_floats_with_nulls_and_nans = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0)), length= 20)),
    ('b', FloatGen(nullable=(True, 10.0), special_cases=[(float('nan'), 10.0)])),
    ('c', LongGen())]

# grouping single-level lists
# StringGen for the value being aggregated will force CUDF to do a sort based aggregation internally.
_grpkey_list_with_non_nested_children = [[('a', RepeatSeqGen(ArrayGen(data_gen), length=3)),
                                          ('b', IntegerGen())] for data_gen in all_basic_gens + decimal_gens] + \
                                        [[('a', RepeatSeqGen(ArrayGen(data_gen), length=3)),
                                          ('b', StringGen())] for data_gen in all_basic_gens + decimal_gens]

#grouping mutliple-level structs with arrays
_grpkey_nested_structs_with_array_basic_child = [[
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', ArrayGen(IntegerGen())]]),
        length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]]

_nan_zero_float_special_cases = [
    (float('nan'),  5.0),
    (NEG_FLOAT_NAN_MIN_VALUE, 5.0),
    (NEG_FLOAT_NAN_MAX_VALUE, 5.0),
    (POS_FLOAT_NAN_MIN_VALUE, 5.0),
    (POS_FLOAT_NAN_MAX_VALUE, 5.0),
    (float('0.0'),  5.0),
    (float('-0.0'), 5.0),
]

_grpkey_floats_with_nan_zero_grouping_keys = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0), special_cases=_nan_zero_float_special_cases), length=50)),
    ('b', IntegerGen(nullable=(True, 10.0))),
    ('c', LongGen())]

_nan_zero_double_special_cases = [
    (float('nan'),  5.0),
    (NEG_DOUBLE_NAN_MIN_VALUE, 5.0),
    (NEG_DOUBLE_NAN_MAX_VALUE, 5.0),
    (POS_DOUBLE_NAN_MIN_VALUE, 5.0),
    (POS_DOUBLE_NAN_MAX_VALUE, 5.0),
    (float('0.0'),  5.0),
    (float('-0.0'), 5.0),
]

_grpkey_doubles_with_nan_zero_grouping_keys = [
    ('a', RepeatSeqGen(DoubleGen(nullable=(True, 10.0), special_cases=_nan_zero_double_special_cases), length=50)),
    ('b', IntegerGen(nullable=(True, 10.0))),
    ('c', LongGen())]

# Schema for xfail cases
struct_gens_xfail = [
    _grpkey_floats_with_nulls_and_nans
]

# List of schemas with NaNs included
_init_list = [
    _longs_with_nulls,
    _longs_with_no_nulls,
    _grpkey_longs_with_nulls,
    _grpkey_dbls_with_nulls,
    _grpkey_floats_with_nulls,
    _grpkey_strings_with_nulls,
    _grpkey_strings_with_extra_nulls,
    _grpkey_nulls,
    _grpkey_floats_with_nulls_and_nans]

# grouping decimals with nulls
_decimals_with_nulls = [('a', DecimalGen()), ('b', DecimalGen()), ('c', DecimalGen())]

# grouping decimals with no nulls
_decimals_with_no_nulls = [
    ('a', DecimalGen(nullable=False)),
    ('b', DecimalGen(nullable=False)),
    ('c', DecimalGen(nullable=False))]

_init_list_with_decimals = _init_list + [
    _decimals_with_nulls, _decimals_with_no_nulls]

# Used to test ANSI-mode fallback
_no_overflow_ansi_gens = [
    ByteGen(min_val = 1, max_val = 10, special_cases=[]),
    ShortGen(min_val = 1, max_val = 100, special_cases=[]),
    IntegerGen(min_val = 1, max_val = 1000, special_cases=[]),
    LongGen(min_val = 1, max_val = 3000, special_cases=[])]

_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)
_decimal_gen_36_neg5 = DecimalGen(precision=36, scale=-5)
_decimal_gen_38_10 = DecimalGen(precision=38, scale=10)


def get_params(init_list, marked_params=[]):
    """
    A method to build the test inputs along with their passed in markers to allow testing
    specific params with their relevant markers. Right now it is used to parametrize _confs with
    allow_non_gpu which allows some operators to be enabled.
    However, this can be used with any list of params to the test.
    :arg init_list list of param values to be tested
    :arg marked_params A list of tuples of (params, list of pytest markers)
    Look at params_markers_for_confs as an example.
    """
    list = init_list.copy()
    for index in range(0, len(list)):
        for test_case, marks in marked_params:
            if list[index] == test_case:
                list[index] = pytest.param(list[index], marks=marks)
    return list


# Run these tests with in 5 modes, all on the GPU
_confs = [_float_conf, _float_smallbatch_conf, _float_conf_skipagg, _float_conf_final, _float_conf_partial]

# Pytest marker for list of operators allowed to run on the CPU,
# esp. useful in partial and final only modes.
# but this ends up allowing close to everything being off the GPU so I am not sure how
# useful this really is
_excluded_operators_marker = pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression', 'UnscaledValue', 'MakeDecimal',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'StddevPop', 'StddevSamp', 'VariancePop', 'VarianceSamp',
    'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec', 'Coalesce', 'IsNull', 'EqualNullSafe',
    'PivotFirst', 'GetArrayItem', 'ShuffleExchangeExec', 'HashPartitioning')

params_markers_for_confs = [
    (_float_conf_partial, [_excluded_operators_marker]),
    (_float_conf_final, [_excluded_operators_marker]),
    (_float_conf, [_excluded_operators_marker])
]

_grpkey_small_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=7, scale=3, nullable=(True, 10.0)), length=50)),
    ('b', DecimalGen(precision=5, scale=2)),
    ('c', DecimalGen(precision=8, scale=3))]

_grpkey_big_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=32, scale=10, nullable=(True, 10.0)), length=50)),
    ('b', DecimalGen(precision=20, scale=2)),
    ('c', DecimalGen(precision=36, scale=5))]

_grpkey_short_mid_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', decimal_gen_64bit),
    ('c', decimal_gen_64bit)]

_grpkey_short_big_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', decimal_gen_128bit),
    ('c', decimal_gen_128bit)]

# NOTE on older versions of Spark decimal 38 causes the CPU to crash
#  instead of detect overflows, we have versions of this for both
#  36 and 38 so we can get some coverage for old versions and full
# coverage for newer versions
_grpkey_short_very_big_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_36_5),
    ('c', _decimal_gen_36_5)]

_grpkey_short_very_big_neg_scale_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_36_neg5),
    ('c', _decimal_gen_36_neg5)]

# Only use negative values to avoid the potential to hover around an overflow
# as values are added and subtracted during the sum. Non-deterministic ordering
# of values from shuffle cannot guarantee overflow calculation is predictable
# when the sum can move in both directions as new partitions are aggregated.
_decimal_gen_sum_38_0 = DecimalGen(precision=38, scale=0, avoid_positive_values=True)
_decimal_gen_sum_38_neg10 = DecimalGen(precision=38, scale=-10, avoid_positive_values=True)

_grpkey_short_sum_full_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_sum_38_0),
    ('c', _decimal_gen_sum_38_0)]

_grpkey_short_sum_full_neg_scale_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_sum_38_neg10),
    ('c', _decimal_gen_sum_38_neg10)]


_init_list_with_decimalbig = _init_list + [
    _grpkey_small_decimals, _grpkey_big_decimals, _grpkey_short_mid_decimals,
    _grpkey_short_big_decimals, _grpkey_short_very_big_decimals,
    _grpkey_short_very_big_neg_scale_decimals]




@approximate_float
@ignore_order
@disable_ansi_mode  # https://github.com/NVIDIA/spark-rapids/issues/5114
@incompat
@pytest.mark.parametrize('data_gen', [_longs_with_nulls], ids=idfn)
@pytest.mark.parametrize('conf', [_float_conf], ids=idfn)
def test_hash_grpby_avg(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=40).groupby('a').agg(f.avg('b')),
        conf=conf
    )
