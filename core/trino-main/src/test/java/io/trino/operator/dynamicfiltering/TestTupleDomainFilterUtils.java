/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.none;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TestingIdType.ID;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static java.lang.Float.POSITIVE_INFINITY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTupleDomainFilterUtils
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @ParameterizedTest
    @MethodSource("testTupleDomainDiscreteValues")
    public void testCreateTupleDomainFilters(Type type, List<Long> values)
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(type, values)));
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS);
        TupleDomainFilter filter = filters.get(column);
        assertThat(filter).isInstanceOf(TupleDomainFilter.LongHashSetFilter.class);
        verifyFilterContainsValues(filter, ImmutableSet.copyOf(values), type);

        assertThat(testContains(filter, -0L, type)).isTrue();
        long minValue;
        long maxValue;
        if (type == DATE) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        }
        else {
            minValue = (long) type.getRange().map(Type.Range::getMin).orElse(Long.MIN_VALUE);
            maxValue = (long) type.getRange().map(Type.Range::getMax).orElse(Long.MAX_VALUE);
        }
        assertThat(testContains(filter, minValue, type)).isFalse();
        assertThat(testContains(filter, maxValue, type)).isFalse();

        long min = values.get(0);
        long max = min;
        for (int i = 1; i < values.size(); i++) {
            min = Math.min(min, values.get(i));
            max = Math.max(max, values.get(i));
        }
        assertThat(testContains(filter, min - 1, type)).isFalse();
        assertThat(testContains(filter, max + 1, type)).isFalse();
        assertThat(testContains(filter, min + 1, type)).isFalse();
        assertThat(testContains(filter, max - 1, type)).isFalse();
    }

    public static Object[][] testTupleDomainDiscreteValues()
    {
        return new Object[][] {
                {INTEGER, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {BIGINT, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {SMALLINT, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIMESTAMP_SECONDS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIMESTAMP_MILLIS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIMESTAMP_MICROS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIME_SECONDS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIME_MILLIS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIME_MICROS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIME_NANOS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {TIME_PICOS, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {DATE, ImmutableList.of(0L, 3L, 500L, 7000L)},
                {createDecimalType(6, 1), ImmutableList.of(0L, 3L, 500L, 7000L)}
        };
    }

    @Test
    public void testUnsupportedNonPrimitiveTypes()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, singleValue(VARBINARY, utf8Slice("abc"))));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, singleValue(JSON, utf8Slice("abc"))));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column, Domain.create(ValueSet.ofRanges(greaterThan(VARCHAR, utf8Slice("abc"))), false)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();
    }

    @Test
    public void testSliceBloomFilter()
    {
        TupleDomainFilter.SliceBloomFilter filter = new TupleDomainFilter.SliceBloomFilter(
                false,
                VARCHAR,
                ImmutableList.of(
                        utf8Slice("Igne"),
                        utf8Slice("natura"),
                        utf8Slice("renovitur"),
                        utf8Slice("integra.")));
        assertThat(testContains(filter, utf8Slice("Igne"))).isTrue();
        assertThat(testContains(filter, utf8Slice("natura"))).isTrue();
        assertThat(testContains(filter, utf8Slice("renovitur"))).isTrue();
        assertThat(testContains(filter, utf8Slice("integra."))).isTrue();

        assertThat(filter.isNullAllowed()).isFalse();
        assertThat(testContains(filter, utf8Slice("natur"))).isFalse();
        assertThat(testContains(filter, utf8Slice("apple"))).isFalse();

        int valuesCount = 10000;
        List<Slice> testValues = new ArrayList<>(valuesCount);
        List<Slice> filterValues = new ArrayList<>((valuesCount / 9) + 1);
        byte base = 0;
        for (int i = 0; i < valuesCount; i++) {
            Slice value = sequentialBytes(base, i);
            testValues.add(value);
            base = (byte) (base + i);
            if (i % 9 == 0) {
                filterValues.add(value);
            }
        }

        filter = new TupleDomainFilter.SliceBloomFilter(false, VARCHAR, filterValues);
        int hits = 0;
        for (int i = 0; i < valuesCount; i++) {
            boolean contains = testContains(filter, testValues.get(i));
            if (i % 9 == 0) {
                // No false negatives
                assertThat(contains).isTrue();
            }
            hits += contains ? 1 : 0;
        }
        assertThat((double) hits / valuesCount).isBetween(0.1, 0.115);
    }

    private static Slice sequentialBytes(byte base, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (base + i);
        }
        return Slices.wrappedBuffer(bytes);
    }

    @Test
    public void testAlwaysFalseFilter()
    {
        TupleDomainFilter filter = new TupleDomainFilter.AlwaysFalse(INTEGER);
        assertThat(testContains(filter, 123, BIGINT)).isFalse();
        filter = new TupleDomainFilter.AlwaysFalse(VARCHAR);
        assertThat(testContains(filter, utf8Slice("123"))).isFalse();
        assertThat(filter.isNullAllowed()).isFalse();
    }

    @Test
    public void testIsNullFilter()
    {
        TupleDomainFilter filter = new TupleDomainFilter.IsNullFilter(INTEGER);
        assertThat(testContains(filter, 123, BIGINT)).isFalse();
        filter = new TupleDomainFilter.IsNullFilter(VARCHAR);
        assertThat(testContains(filter, utf8Slice("123"))).isFalse();
        assertThat(filter.isNullAllowed()).isTrue();
    }

    @Test
    public void testIsNotNullFilter()
    {
        TupleDomainFilter filter = new TupleDomainFilter.IsNotNullFilter(INTEGER);
        assertThat(testContains(filter, 123, BIGINT)).isTrue();
        filter = new TupleDomainFilter.IsNotNullFilter(VARCHAR);
        assertThat(testContains(filter, utf8Slice("123"))).isTrue();
        assertThat(filter.isNullAllowed()).isFalse();
    }

    @Test
    public void testBooleanType()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, singleValue(BOOLEAN, true)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column, Domain.create(ValueSet.ofRanges(greaterThan(BOOLEAN, true)), false)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();
    }

    @Test
    public void testDoubleType()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(DOUBLE, ImmutableList.of(-0.0, 0.0, 1.0))));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column, Domain.create(ValueSet.ofRanges(greaterThan(DOUBLE, 0.0)), false)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();
    }

    @Test
    public void testRealType()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.ofRanges(greaterThan(REAL, (long) floatToRawIntBits(3.0f))), false)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        List<Long> values = ImmutableList.of(
                (long) floatToRawIntBits(Float.NEGATIVE_INFINITY),
                (long) floatToRawIntBits(-3.0f),
                (long) floatToRawIntBits(-5.0f),
                (long) floatToRawIntBits(0.0f));
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, multipleValues(REAL, values))),
                TYPE_OPERATORS);
        TupleDomainFilter filter = filters.get(column);
        assertThat(filter).isInstanceOf(TupleDomainFilter.LongCustomHashSetFilter.class);
        verifyFilterContainsValues(filter, ImmutableSet.copyOf(values), REAL);
        assertThat(testContains(filter, floatToRawIntBits(POSITIVE_INFINITY), REAL)).isFalse();
        assertThat(testContains(filter, floatToRawIntBits(-0.0f), REAL)).isTrue();
        assertThat(testContains(filter, floatToRawIntBits(0.1f), REAL)).isFalse();
    }

    @Test
    public void testLongDecimalType()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        column,
                        singleValue(createDecimalType(20, 10), Int128.valueOf(2342342343L))));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.ofRanges(greaterThan(
                        createDecimalType(20, 10),
                        Int128.valueOf(2342342343L))), false)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS)).isEmpty();
    }

    @Test
    public void testShortDecimalRange()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(
                        ValueSet.ofRanges(greaterThanOrEqual(createDecimalType(6, 1), 234243L)),
                        false)));
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS);
        assertThat(filters).isEqualTo(ImmutableMap.of(
                column,
                new TupleDomainFilter.LongRangeFilter(false, createDecimalType(6, 1), 234243L, Long.MAX_VALUE)));
    }

    @Test
    public void testLongBitSetFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<Long> values = ImmutableList.of(0L, 5L, 60L, 90L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(TINYINT, values)));
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(tupleDomain, TYPE_OPERATORS);
        TupleDomainFilter filter = new TupleDomainFilter.LongBitSetFilter(false, TINYINT, values, 0L, 90L);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, filter));
        verifyFilterValues(values, filter, 0, 100);

        // (max - min) larger than bitset capacity
        filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(BIGINT, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE)))),
                TYPE_OPERATORS);
        assertThat(filters.get(column)).isInstanceOf(TupleDomainFilter.LongHashSetFilter.class);
        verifyFilterContainsValues(filters.get(column), ImmutableSet.of(Long.MIN_VALUE, Long.MAX_VALUE), BIGINT);
    }

    @Test
    public void testCreateFilterFromAll()
    {
        Map<ColumnHandle, TupleDomainFilter> filters =
                TupleDomainFilterUtils.createTupleDomainFilters(TupleDomain.all(), TYPE_OPERATORS);
        assertThat(filters).isEmpty();
    }

    @Test
    public void testCreateFilterFromOnlyNullDomain()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Type type = INTEGER;
        Map<ColumnHandle, TupleDomainFilter> filters =
                TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(column, onlyNull(type))),
                        TYPE_OPERATORS);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.IsNullFilter(type)));
    }

    @Test
    public void testCreateFilterFromSingleValueDomain()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Type type = INTEGER;
        Map<ColumnHandle, TupleDomainFilter> filters =
                TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(column, singleValue(type, 100L))),
                        TYPE_OPERATORS);
        assertThat(filters).isEqualTo(ImmutableMap.of(
                column,
                new TupleDomainFilter.LongRangeFilter(false, type, 100L, 100L)));
    }

    @Test
    public void testCreateFilterFromNotNullDomain()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Map<ColumnHandle, TupleDomainFilter> filters =
                TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(column, notNull(INTEGER))),
                        TYPE_OPERATORS);
        assertThat(filters).isEqualTo(
                ImmutableMap.of(column, new TupleDomainFilter.IsNotNullFilter(INTEGER)));
    }

    @Test
    public void testLongRangeFilter()
    {
        // greaterThan range
        ColumnHandle column = new TestingColumnHandle("column");
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 0L)), false))),
                TYPE_OPERATORS);
        TupleDomainFilter.LongRangeFilter rangeFilter = new TupleDomainFilter.LongRangeFilter(false, BIGINT, 1L, Long.MAX_VALUE);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, rangeFilter));
        assertThat(rangeFilter.isNullAllowed()).isFalse();
        assertThat(testContains(rangeFilter, 0, BIGINT)).isFalse();
        assertThat(testContains(rangeFilter, 1, BIGINT)).isTrue();
        assertThat(testContains(rangeFilter, Long.MAX_VALUE, BIGINT)).isTrue();

        // lessThan range
        filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L)), false))),
                TYPE_OPERATORS);
        rangeFilter = new TupleDomainFilter.LongRangeFilter(false, BIGINT, Long.MIN_VALUE, 99L);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, rangeFilter));
        assertThat(testContains(rangeFilter, 100, BIGINT)).isFalse();
        assertThat(testContains(rangeFilter, 99, BIGINT)).isTrue();
        assertThat(testContains(rangeFilter, Long.MIN_VALUE, BIGINT)).isTrue();

        // [low, high] inclusive range
        filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.ofRanges(range(INTEGER, 5L, true, 10L, true)), false))),
                TYPE_OPERATORS);
        rangeFilter = new TupleDomainFilter.LongRangeFilter(false, INTEGER, 5L, 10L);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, rangeFilter));
        for (int i = 5; i <= 10; i++) {
            assertThat(testContains(rangeFilter, i, INTEGER)).isTrue();
        }

        // (low, high) exclusive range
        filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.ofRanges(range(INTEGER, 5L, false, 10L, false)), true))),
                TYPE_OPERATORS);
        rangeFilter = new TupleDomainFilter.LongRangeFilter(true, INTEGER, 6L, 9L);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, rangeFilter));
        assertThat(testContains(rangeFilter, 5, INTEGER)).isFalse();
        assertThat(testContains(rangeFilter, 10, INTEGER)).isFalse();
        assertThat(rangeFilter.isNullAllowed()).isTrue();
        for (int i = 6; i <= 9; i++) {
            assertThat(testContains(rangeFilter, i, INTEGER)).isTrue();
        }

        // empty range with null allowed
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(range(INTEGER, 1L, false, 2L, false)), true))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.IsNullFilter(INTEGER)));

        // empty range with null not allowed
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(range(INTEGER, 1L, false, 2L, false)), false))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.AlwaysFalse(INTEGER)));

        // all without null
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(Range.all(INTEGER)), false))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.IsNotNullFilter(INTEGER)));

        // all with null allowed
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.ofRanges(Range.all(INTEGER)), true))),
                TYPE_OPERATORS)).isEmpty();

        // greater than MAX
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, Long.MAX_VALUE)), false))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.AlwaysFalse(BIGINT)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, Long.MAX_VALUE)), true))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.IsNullFilter(BIGINT)));

        // less than MIN
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(lessThan(BIGINT, Long.MIN_VALUE)), false))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.AlwaysFalse(BIGINT)));
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.ofRanges(lessThan(BIGINT, Long.MIN_VALUE)), true))),
                        TYPE_OPERATORS))
                .isEqualTo(ImmutableMap.of(column, new TupleDomainFilter.IsNullFilter(BIGINT)));
    }

    @Test
    public void testRangeFilterFromPackedDomain()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<Long> values = ImmutableList.of(1231L, 1232L, 1233L, 1234L, 1235L);
        Map<ColumnHandle, TupleDomainFilter> filters = TupleDomainFilterUtils.createTupleDomainFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, multipleValues(BIGINT, values))),
                TYPE_OPERATORS);
        TupleDomainFilter filter = new TupleDomainFilter.LongRangeFilter(false, BIGINT, 1231L, 1235L);
        assertThat(filters).isEqualTo(ImmutableMap.of(column, filter));
        verifyFilterValues(values, filter, 0, 2000);
    }

    @Test
    public void testCreateFilterFromAllOrNoneValueSet()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column, Domain.create(ValueSet.all(HYPER_LOG_LOG), true))),
                        TYPE_OPERATORS))
                .isEmpty();
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column, Domain.create(ValueSet.all(HYPER_LOG_LOG), false))),
                        TYPE_OPERATORS))
                .isEmpty();
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column, Domain.create(ValueSet.none(HYPER_LOG_LOG), true))),
                        TYPE_OPERATORS))
                .isEmpty();
    }

    @Test
    public void testCreateFilterFromEquatableValueSet()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column, Domain.create(ValueSet.of(ID, 1L, 2L, 3L), true))),
                        TYPE_OPERATORS))
                .isEmpty();
        Assertions.assertThat(TupleDomainFilterUtils.createTupleDomainFilters(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                column, Domain.create(ValueSet.of(ID, 1L, 2L, 3L), false))),
                        TYPE_OPERATORS))
                .isEmpty();
    }

    @Test
    public void testCreateFilterFromNone()
    {
        assertThatThrownBy(() -> TupleDomainFilterUtils.createTupleDomainFilters(none(), TYPE_OPERATORS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("tupleDomain is none");
    }

    @Test
    public void testTooLargeBitSetFilter()
    {
        assertThatThrownBy(() -> new TupleDomainFilter.LongBitSetFilter(true, INTEGER, ImmutableList.of(0L), 0, ((long) Integer.MAX_VALUE) + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Values range 2147483649 is outside integer range");
    }

    private static boolean testContains(TupleDomainFilter filter, long value, Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.writeLong(blockBuilder, value);
        return filter.testContains(blockBuilder.build(), 0);
    }

    private static boolean testContains(TupleDomainFilter filter, Slice value)
    {
        VariableWidthBlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(blockBuilder, value);
        return filter.testContains(blockBuilder.build(), 0);
    }

    private static void verifyFilterValues(List<Long> values, TupleDomainFilter filter, int start, int end)
    {
        Set<Long> valuesSet = ImmutableSet.copyOf(values);
        Block block = createLongSequenceBlock(start, end, filter.getType());
        for (long i = 0; i < block.getPositionCount(); i++) {
            assertThat(filter.testContains(block, toIntExact(i))).isEqualTo(valuesSet.contains(i));
        }
    }

    private static void verifyFilterContainsValues(TupleDomainFilter filter, Set<?> values, Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            TypeUtils.writeNativeValue(type, blockBuilder, value);
        }
        Block block = blockBuilder.build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(filter.testContains(block, i)).isTrue();
        }
    }
}
