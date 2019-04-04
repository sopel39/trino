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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectOperator
        extends AbstractScanFilterAndProjectOperator
        implements SourceOperator, Closeable
{
    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final SettableFuture<?> blockedOnSplits = SettableFuture.create();

    private WorkProcessor<Page> pages;

    private Split split;
    private boolean operatorFinishing;

    private ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        super(
                operatorContext.getSession(),
                operatorContext.getDriverContext().getYieldSignal(),
                pageSourceProvider,
                cursorProcessor,
                pageProcessor,
                table,
                columns,
                types,
                minOutputPageSize,
                minOutputPageRowCount);

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(sourceId, "sourceId is null");

        pages = WorkProcessor.create(
                new SplitToPages(operatorContext.aggregateSystemMemoryContext()))
                .transformProcessor(WorkProcessor::flatten)
                .finishWhen(() -> operatorFinishing);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (operatorFinishing) {
            return Optional::empty;
        }

        this.split = split;

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }
        blockedOnSplits.set(null);

        return getUpdatablePageSourceSupplier();
    }

    @Override
    public void noMoreSplits()
    {
        blockedOnSplits.set(null);
    }

    @Override
    public void close()
    {
        super.close();
    }

    @Override
    public void finish()
    {
        blockedOnSplits.set(null);
        operatorFinishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (pages.isBlocked()) {
            return pages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return false;
    }

    @Override
    public final void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return pages.getResult();
    }

    private class SplitToPages
            implements WorkProcessor.Process<WorkProcessor<Page>>
    {
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;

        boolean finished;

        SplitToPages(AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
        }

        @Override
        public ProcessState<WorkProcessor<Page>> process()
        {
            if (finished) {
                memoryContext.close();
                return ProcessState.finished();
            }

            if (!blockedOnSplits.isDone()) {
                return ProcessState.blocked(blockedOnSplits);
            }

            finished = true;

            WorkProcessor<Page> pages = ScanFilterAndProjectOperator.this.processSplit(
                    split,
                    localAggregatedMemoryContext,
                    pageSourceMemoryContext,
                    outputMemoryContext);
            pages = pages.withProcessStateMonitor(state -> {
                memoryContext.setBytes(localAggregatedMemoryContext.getBytes());
                long deltaPositionsCount = getAndResetDeltaPositionsCount();
                operatorContext.recordPhysicalInputWithTiming(
                        getAndResetDeltaPhysicalBytes(),
                        deltaPositionsCount,
                        getAndResetDeltaPhysicalReadTimeNanos());
                operatorContext.recordProcessedInput(
                        getAndResetDeltaProcessedBytes(), deltaPositionsCount);
            });
            return ProcessState.ofResult(pages);
        }
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Supplier<PageProcessor> pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ScanFilterAndProjectOperator.class.getSimpleName());
            return new ScanFilterAndProjectOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.get(),
                    table,
                    columns,
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
