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
package io.trino.operator;

import io.trino.execution.ScheduledSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.OptionalInt;

<<<<<<< HEAD
public interface DriverFactory
=======
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * {@link SplitDriverFactory} that has predefined list of {@link OperatorFactory}ies that does not depend on a particular split.
 */
public class DriverFactory
        implements SplitDriverFactory
>>>>>>> 8a6d39c0156 (Wait for splits)
{
    int getPipelineId();

    boolean isInputDriver();

    boolean isOutputDriver();

    OptionalInt getDriverInstances();

    Driver createDriver(DriverContext driverContext, Optional<ScheduledSplit> split);

    void noMoreDrivers();

    boolean isNoMoreDrivers();

    void localPlannerComplete();

    /**
     * return the sourceId of this DriverFactory.
     * A DriverFactory doesn't always have source node.
     * For example, ValuesNode is not a source node.
     */
<<<<<<< HEAD
    Optional<PlanNodeId> getSourceId();
=======
    @Override
    public Optional<PlanNodeId> getSourceId()
    {
        return sourceId;
    }

    @Override
    public OptionalInt getDriverInstances()
    {
        return driverInstances;
    }

    @Override
    public DriverWithFuture createDriver(DriverContext driverContext, Optional<ScheduledSplit> split)
    {
        requireNonNull(driverContext, "driverContext is null");
        List<Operator> operators = new ArrayList<>(operatorFactories.size());
        try {
            synchronized (this) {
                // must check noMoreDrivers after acquiring the lock
                checkState(!noMoreDrivers, "noMoreDrivers is already set");
                for (OperatorFactory operatorFactory : operatorFactories) {
                    Operator operator = operatorFactory.createOperator(driverContext);
                    operators.add(operator);
                }
            }
            // Driver creation can continue without holding the lock
            return new DriverWithFuture(Optional.of(Driver.createDriver(driverContext, operators)), immediateVoidFuture());
        }
        catch (Throwable failure) {
            for (Operator operator : operators) {
                try {
                    operator.close();
                }
                catch (Throwable closeFailure) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            for (OperatorContext operatorContext : driverContext.getOperatorContexts()) {
                try {
                    operatorContext.destroy();
                }
                catch (Throwable destroyFailure) {
                    if (failure != destroyFailure) {
                        failure.addSuppressed(destroyFailure);
                    }
                }
            }
            driverContext.failed(failure);
            throw failure;
        }
    }

    @Override
    public synchronized void noMoreDrivers()
    {
        if (noMoreDrivers) {
            return;
        }
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.noMoreOperators();
        }
        noMoreDrivers = true;
    }

    // no need to synchronize when just checking the boolean flag
    @SuppressWarnings("GuardedBy")
    @Override
    public boolean isNoMoreDrivers()
    {
        return noMoreDrivers;
    }

    @Override
    public void localPlannerComplete()
    {
        operatorFactories
                .stream()
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);
    }
>>>>>>> 8a6d39c0156 (Wait for splits)
}
