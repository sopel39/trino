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
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.CursorProcessorOutput;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;

class AbstractScanFilterAndProjectOperator
        implements Closeable
{
    private final Session session;
    private final DriverYieldSignal yieldSignal;
    private final PageSourceProvider pageSourceProvider;
    private final CursorProcessor cursorProcessor;
    private final PageProcessor pageProcessor;
    private final TableHandle table;
    private final List<ColumnHandle> columns;
    private final List<Type> types;
    private final DataSize minOutputPageSize;
    private final int minOutputPageRowCount;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private long deltaPositionsCount;
    private long deltaPhysicalBytes;
    private long deltaPhysicalReadTimeNanos;
    private long deltaProcessedBytes;

    AbstractScanFilterAndProjectOperator(
            Session session,
            DriverYieldSignal yieldSignal,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        this.session = requireNonNull(session, "session is null");
        this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
        this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
        this.minOutputPageRowCount = minOutputPageRowCount;
    }

    @Override
    public void close()
    {
        if (pageSource != null) {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else if (cursor != null) {
            cursor.close();
        }
    }

    long getAndResetDeltaPositionsCount()
    {
        long currentDeltaPositionsCount = deltaPositionsCount;
        deltaPositionsCount = 0;
        return currentDeltaPositionsCount;
    }

    long getAndResetDeltaPhysicalBytes()
    {
        long currentDeltaPhysicalBytes = deltaPhysicalBytes;
        deltaPhysicalBytes = 0;
        return currentDeltaPhysicalBytes;
    }

    long getAndResetDeltaPhysicalReadTimeNanos()
    {
        long currentDeltaPhysicalReadTimeNanos = deltaPhysicalReadTimeNanos;
        deltaPhysicalReadTimeNanos = 0;
        return currentDeltaPhysicalReadTimeNanos;
    }

    long getAndResetDeltaProcessedBytes()
    {
        long currentDeltaProcessedBytes = deltaProcessedBytes;
        deltaProcessedBytes = 0;
        return currentDeltaProcessedBytes;
    }

    Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return () -> {
            if (pageSource instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) pageSource);
            }
            return Optional.empty();
        };
    }

    WorkProcessor<Page> processSplit(
            Split split,
            AggregatedMemoryContext aggregatedMemoryContext,
            LocalMemoryContext pageSourceMemoryContext,
            LocalMemoryContext outputMemoryContext)
    {
        ConnectorPageSource source;
        if (split.getConnectorSplit() instanceof EmptySplit) {
            source = new EmptySplitPageSource();
        }
        else {
            source = pageSourceProvider.createPageSource(session, split, table, columns);
        }

        if (source instanceof RecordPageSource) {
            cursor = ((RecordPageSource) source).getCursor();
            return processColumnSource(pageSourceMemoryContext, outputMemoryContext);
        }
        else {
            pageSource = source;
            return processPageSource(aggregatedMemoryContext, pageSourceMemoryContext, outputMemoryContext);
        }
    }

    private WorkProcessor<Page> processColumnSource(
            LocalMemoryContext pageSourceMemoryContext,
            LocalMemoryContext outputMemoryContext)
    {
        return WorkProcessor
                .create(new RecordCursorToPages(cursorProcessor, types, pageSourceMemoryContext, outputMemoryContext))
                .yielding(yieldSignal::isSet);
    }

    private WorkProcessor<Page> processPageSource(
            AggregatedMemoryContext aggregatedMemoryContext,
            LocalMemoryContext pageSourceMemoryContext,
            LocalMemoryContext outputMemoryContext)
    {
        return WorkProcessor
                .create(new ConnectorPageSourceToPages(pageSourceMemoryContext))
                .yielding(yieldSignal::isSet)
                .flatMap(page -> pageProcessor.createWorkProcessor(
                        session.toConnectorSession(),
                        yieldSignal,
                        outputMemoryContext,
                        page))
                .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, aggregatedMemoryContext));
    }

    private class RecordCursorToPages
            implements WorkProcessor.Process<Page>
    {
        final CursorProcessor cursorProcessor;
        final PageBuilder pageBuilder;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;

        long completedBytes;
        long readTimeNanos;
        boolean finished;

        RecordCursorToPages(CursorProcessor cursorProcessor, List<Type> types, LocalMemoryContext pageSourceMemoryContext, LocalMemoryContext outputMemoryContext)
        {
            this.cursorProcessor = cursorProcessor;
            this.pageBuilder = new PageBuilder(types);
            this.pageSourceMemoryContext = pageSourceMemoryContext;
            this.outputMemoryContext = outputMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (!finished) {
                CursorProcessorOutput output = cursorProcessor.process(session.toConnectorSession(), yieldSignal, cursor, pageBuilder);
                pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

                long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
                long elapsedNanos = cursor.getReadTimeNanos() - readTimeNanos;
                // TODO: derive better values for cursors
                deltaPositionsCount += output.getProcessedRows();
                deltaPhysicalBytes += bytesProcessed;
                deltaPhysicalReadTimeNanos += elapsedNanos;
                deltaProcessedBytes += bytesProcessed;
                completedBytes = cursor.getCompletedBytes();
                readTimeNanos = cursor.getReadTimeNanos();
                if (output.isNoMoreRows()) {
                    finished = true;
                }
            }

            if (pageBuilder.isFull() || (finished && !pageBuilder.isEmpty())) {
                // only return a page if buffer is full or cursor has finished
                Page page = pageBuilder.build();
                pageBuilder.reset();
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ProcessState.ofResult(page);
            }
            else if (finished) {
                checkState(pageBuilder.isEmpty());
                return ProcessState.finished();
            }
            else {
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ProcessState.yield();
            }
        }
    }

    private class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final LocalMemoryContext pageSourceMemoryContext;

        long completedBytes;
        long readTimeNanos;

        ConnectorPageSourceToPages(LocalMemoryContext pageSourceMemoryContext)
        {
            this.pageSourceMemoryContext = pageSourceMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            page = recordProcessedInput(page);

            // update operator stats
            long endCompletedBytes = pageSource.getCompletedBytes();
            long endReadTimeNanos = pageSource.getReadTimeNanos();
            deltaPositionsCount += page.getPositionCount();
            deltaPhysicalBytes += endCompletedBytes - completedBytes;
            deltaPhysicalReadTimeNanos += endReadTimeNanos - readTimeNanos;
            completedBytes = endCompletedBytes;
            readTimeNanos = endReadTimeNanos;

            return ProcessState.ofResult(page);
        }
    }

    private Page recordProcessedInput(Page page)
    {
        // account processed bytes from lazy blocks only when they are loaded
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            if (block instanceof LazyBlock) {
                LazyBlock delegateLazyBlock = (LazyBlock) block;
                blocks[i] = new LazyBlock(page.getPositionCount(), lazyBlock -> {
                    Block loadedBlock = delegateLazyBlock.getLoadedBlock();
                    deltaProcessedBytes += loadedBlock.getSizeInBytes();
                    lazyBlock.setBlock(loadedBlock);
                });
            }
            else {
                deltaProcessedBytes += block.getSizeInBytes();
                blocks[i] = block;
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
