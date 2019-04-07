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
import io.prestosql.Session;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.split.PageSourceProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class TableScanWorkProcessorOperator
        implements WorkProcessorSourceOperator
{
    private final int operatorId;
    private final WorkProcessor<Page> pages;

    private ConnectorPageSource source;

    public TableScanWorkProcessorOperator(
            int operatorId,
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Split> splits,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns)
    {
        this.operatorId = operatorId;
        this.pages = splits.flatTransform(new SplitToPages(
                session,
                pageSourceProvider,
                table,
                columns,
                memoryTrackingContext.aggregateSystemMemoryContext()));
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
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return () -> {
            if (source instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) source);
            }
            return Optional.empty();
        };
    }

    @Override
    public void close()
            throws Exception
    {
        if (source != null) {
            try {
                source.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final PageSourceProvider pageSourceProvider;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final AggregatedMemoryContext aggregatedMemoryContext;

        private SplitToPages(
                Session session,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.aggregatedMemoryContext = requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                return TransformationState.finished();
            }

            source = pageSourceProvider.createPageSource(session, split, table, columns);
            return TransformationState.ofResult(
                    WorkProcessor.create(new ConnectorPageSourceToPages(aggregatedMemoryContext, source)));
        }
    }

    private class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final ConnectorPageSource pageSource;
        final LocalMemoryContext memoryContext;

        ConnectorPageSourceToPages(AggregatedMemoryContext aggregatedMemoryContext, ConnectorPageSource pageSource)
        {
            this.pageSource = pageSource;
            this.memoryContext = aggregatedMemoryContext
                    .newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                memoryContext.close();
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            memoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            // TODO: report operator stats
            return ProcessState.ofResult(page);
        }
    }
}
