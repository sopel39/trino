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
package io.prestosql.plugin.hive;

import io.prestosql.orc.metadata.CompressionKind;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import parquet.hadoop.metadata.CompressionCodecName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public enum HiveCompressionCodec
{
    NONE(null, CompressionKind.NONE, CompressionCodecName.UNCOMPRESSED),
    SNAPPY(SnappyCodec.class, CompressionKind.SNAPPY, CompressionCodecName.SNAPPY),
    GZIP(GzipCodec.class, CompressionKind.ZLIB, CompressionCodecName.GZIP);

    private final Optional<Class<? extends CompressionCodec>> codec;
    private final CompressionKind orcCompressionKind;
    private final CompressionCodecName parquetCompressionCodec;

    HiveCompressionCodec(Class<? extends CompressionCodec> codec, CompressionKind orcCompressionKind, CompressionCodecName parquetCompressionCodec)
    {
        this.codec = Optional.ofNullable(codec);
        this.orcCompressionKind = requireNonNull(orcCompressionKind, "orcCompressionKind is null");
        this.parquetCompressionCodec = requireNonNull(parquetCompressionCodec, "parquetCompressionCodec is null");
    }

    public Optional<Class<? extends CompressionCodec>> getCodec()
    {
        return codec;
    }

    public CompressionKind getOrcCompressionKind()
    {
        return orcCompressionKind;
    }

    public CompressionCodecName getParquetCompressionCodec()
    {
        return parquetCompressionCodec;
    }
}
