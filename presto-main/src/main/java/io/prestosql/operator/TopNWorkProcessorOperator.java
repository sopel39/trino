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

import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;

import java.util.List;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNWorkProcessorOperator
        extends AbstractTopNOperator
        implements WorkProcessorOperator
{
    private final int operatorId;
    private final WorkProcessor<Page> pages;

    public TopNWorkProcessorOperator(
            int operatorId,
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Page> sourcePages,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        super(
                memoryTrackingContext.aggregateUserMemoryContext(),
                types,
                n,
                sortChannels,
                sortOrders);

        this.operatorId = operatorId;

        if (n == 0) {
            pages = WorkProcessor.of();
        }
        else {
            pages = sourcePages.transform(new TopNPages());
        }
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
            throws Exception
    {}

    private class TopNPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (inputPage == null) {
                Page page = null;
                while (page == null && !noMoreOutput()) {
                    page = getOutput();
                }

                if (page != null) {
                    return TransformationState.ofResult(page, false);
                }

                return TransformationState.finished();
            }

            addInput(inputPage);
            return TransformationState.needsMoreData();
        }
    }
}
