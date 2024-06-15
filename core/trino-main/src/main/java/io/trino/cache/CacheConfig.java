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
package io.trino.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;

public class CacheConfig
{
    private boolean enabled;
    private double revokingThreshold = 0.9;
    private double revokingTarget = 0.7;
    private boolean cacheCommonSubqueriesEnabled = true;
    private boolean cacheAggregationsEnabled = true;
    private boolean cacheProjectionsEnabled = true;
    private DataSize maxSplitSize = DataSize.of(256, DataSize.Unit.MEGABYTE);
    // The minimum number of splits with distinct CacheSplitID that should be processed by a worker
    // before scheduling the next batch of splits which can contain splits with the same CacheSplitID.
    // We have to set this such that there is a sufficient gap between the splits with the same CacheSplitID
    // considering 128 splits can be processed by a worker in parallel (in case of 32 core machines). Furthermore,
    // we have to consider that from second batch onwards, some splits will get processed much faster since
    // they are cached. Additionally, there might be some non-determinism in the scheduling. Hence, the gap
    // should be a bit more than 128. Experimentally, we found that 500 is a good value.
    private int cacheMinWorkerSplitSeparation = 500;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Enables pipeline level cache")
    public CacheConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingThreshold()
    {
        return revokingThreshold;
    }

    @Config("cache.revoking-threshold")
    @ConfigDescription("Revoke cache memory when memory pool is filled over threshold")
    public CacheConfig setRevokingThreshold(double revokingThreshold)
    {
        this.revokingThreshold = revokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingTarget()
    {
        return revokingTarget;
    }

    @Config("cache.revoking-target")
    @ConfigDescription("When revoking cache memory, revoke so much that cache memory reservation is below target at the end")
    public CacheConfig setRevokingTarget(double revokingTarget)
    {
        this.revokingTarget = revokingTarget;
        return this;
    }

    public boolean isCacheCommonSubqueriesEnabled()
    {
        return cacheCommonSubqueriesEnabled;
    }

    @Config("cache.common-subqueries.enabled")
    @LegacyConfig("cache.subqueries.enabled")
    @ConfigDescription("Enables caching of common subqueries when running a single query")
    public CacheConfig setCacheCommonSubqueriesEnabled(boolean cacheCommonSubqueriesEnabled)
    {
        this.cacheCommonSubqueriesEnabled = cacheCommonSubqueriesEnabled;
        return this;
    }

    public boolean isCacheAggregationsEnabled()
    {
        return cacheAggregationsEnabled;
    }

    @Config("cache.aggregations.enabled")
    @ConfigDescription("Enables caching of aggregations")
    public CacheConfig setCacheAggregationsEnabled(boolean cacheAggregationsEnabled)
    {
        this.cacheAggregationsEnabled = cacheAggregationsEnabled;
        return this;
    }

    public boolean isCacheProjectionsEnabled()
    {
        return cacheProjectionsEnabled;
    }

    @Config("cache.projections.enabled")
    @ConfigDescription("Enables caching of projections")
    public CacheConfig setCacheProjectionsEnabled(boolean cacheProjectionsEnabled)
    {
        this.cacheProjectionsEnabled = cacheProjectionsEnabled;
        return this;
    }

    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("cache.max-split-size")
    @ConfigDescription("Upper bound for size of cached split")
    public CacheConfig setMaxSplitSize(DataSize cacheSubqueriesSize)
    {
        this.maxSplitSize = cacheSubqueriesSize;
        return this;
    }

    @Min(0)
    public int getCacheMinWorkerSplitSeparation()
    {
        return cacheMinWorkerSplitSeparation;
    }

    @Config("cache.min-worker-split-separation")
    @ConfigDescription("The minimum separation (in terms of processed splits) between two splits with same cache split id being scheduled on the single worker")
    public CacheConfig setCacheMinWorkerSplitSeparation(int cacheMinWorkerSplitSeparation)
    {
        this.cacheMinWorkerSplitSeparation = cacheMinWorkerSplitSeparation;
        return this;
    }
}
