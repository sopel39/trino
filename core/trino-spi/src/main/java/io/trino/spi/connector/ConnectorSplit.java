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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;

import java.util.List;
import java.util.Map;

public interface ConnectorSplit
{
    /**
     * Returns true if this ConnectorSplit could be scheduled on any node.
     * If {@link #getAddresses()} is not empty, then split will be scheduled
     * on a node with one of the addresses returned by {@link #getAddresses()}
     * if such node is part of the cluster. Otherwise, the split will be scheduled on any node.
     * <p>
     * When this method returns false, the split will always be scheduled on a node with one
     * of the addresses returned by {@link #getAddresses()}. Query will fail if such node is
     * not available.
     */
    default boolean isRemotelyAccessible()
    {
        return true;
    }

    default List<HostAddress> getAddresses()
    {
        if (!isRemotelyAccessible()) {
            throw new IllegalStateException("getAddresses must be implemented when for splits with isRemotelyAccessible=false");
        }
        return List.of();
    }

    @JsonIgnore // ConnectorSplit is json-serializable, but we don't want to repeat information in that field
    default Map<String, String> getSplitInfo()
    {
        return Map.of();
    }

    @Deprecated(forRemoval = true)
    @JsonIgnore
    default Object getInfo()
    {
        throw new UnsupportedOperationException("getInfo is deprecated and will be removed in the future. Use getSplitInfo instead.");
    }

    default SplitWeight getSplitWeight()
    {
        return SplitWeight.standard();
    }

    default long getRetainedSizeInBytes()
    {
        throw new UnsupportedOperationException("This connector does not provide memory accounting capabilities for ConnectorSplit");
    }
}
