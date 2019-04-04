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

import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.PageSourceProvider;

import java.util.Optional;
import java.util.function.Supplier;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectWorkProcessorOperator
        extends AbstractScanFilterAndProjectOperator
        implements WorkProcessorSourceOperator
{
    private final int operatorId;
    private final WorkProcessor<Page> pages;

    ScanFilterAndProjectWorkProcessorOperator(
            int operatorId,
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            DriverYieldSignal yieldSignal,
            WorkProcessor<Split> splits,
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
                session,
                yieldSignal,
                pageSourceProvider,
                cursorProcessor,
                pageProcessor,
                table,
                columns,
                types,
                minOutputPageSize,
                minOutputPageRowCount);
        requireNonNull(memoryTrackingContext, "memoryTrackingContext is null");
        this.operatorId = operatorId;

        pages = splits.flatTransform(
                new SplitToPages(memoryTrackingContext.aggregateSystemMemoryContext()));
    }

    @Override
    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
    {
        super.close();
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return null;
    }

    private class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;

        SplitToPages(AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectWorkProcessorOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectWorkProcessorOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectWorkProcessorOperator.class.getSimpleName());
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return TransformationState.finished();
            }

            WorkProcessor<Page> pages = ScanFilterAndProjectWorkProcessorOperator.this.processSplit(
                    split,
                    localAggregatedMemoryContext,
                    pageSourceMemoryContext,
                    outputMemoryContext);
            pages = pages.withProcessStateMonitor(state -> {
                memoryContext.setBytes(localAggregatedMemoryContext.getBytes());
                // TODO: report operator stats
            });
            return TransformationState.ofResult(pages);
        }
    }
}
