/*
 * Copyright 2023 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.api.MySqlColumnMetadata;
import io.asyncer.r2dbc.mysql.api.MySqlNativeTypeMetadata;
import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.asyncer.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.r2dbc.spi.Nullability;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlColumnMetadata}.
 * <p>
 * Note: same as parameter metadata in MySQL, but parameter metadata is useless for the SPI of R2DBC.
 */
final class MySqlColumnDescriptor implements MySqlColumnMetadata {

    private final int index;

    private final MySqlTypeMetadata typeMetadata;

    private final MySqlType type;

    private final String name;

    private final Nullability nullability;

    private final long size;

    private final int decimals;

    private final int collationId;

    @VisibleForTesting
    MySqlColumnDescriptor(int index, short typeId, String name, int definitions,
        long size, int decimals, int collationId, @Nullable String extendedTypeInfo) {
        require(index >= 0, "index must not be a negative integer");
        require(size >= 0, "size must not be a negative integer");
        require(decimals >= 0, "decimals must not be a negative integer");
        requireNonNull(name, "name must not be null");

        MySqlTypeMetadata typeMetadata = new MySqlTypeMetadata(typeId, definitions, collationId, extendedTypeInfo);

        this.index = index;
        this.typeMetadata = typeMetadata;
        this.type = MySqlType.of(typeMetadata);
        this.name = name;
        this.nullability = typeMetadata.isNotNull() ? Nullability.NON_NULL : Nullability.NULLABLE;
        this.size = size;
        this.decimals = decimals;
        this.collationId = collationId;
    }

    static MySqlColumnDescriptor create(int index, DefinitionMetadataMessage message) {
        int definitions = message.getDefinitions();
        return new MySqlColumnDescriptor(index, message.getTypeId(), message.getColumn(), definitions,
            message.getSize(), message.getDecimals(), message.getCollationId(), message.getExtendedTypeInfo());
    }

    int getIndex() {
        return index;
    }

    @Override
    public MySqlType getType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MySqlNativeTypeMetadata getNativeTypeMetadata() {
        return typeMetadata;
    }

    @Override
    public Nullability getNullability() {
        return nullability;
    }

    @Override
    public Integer getPrecision() {
        // FIXME: NEW_DECIMAL and DECIMAL are "exact" fixed-point number.
        // So the `size` have to subtract:
        // 1. if signed, 1 byte for the sign
        // 2. if decimals > 0, 1 byte for the dot
        return (int) size;
    }

    @Override
    public Integer getScale() {
        // 0x00 means it is an integer or a static string.
        // 0x1f means it is a dynamic string, an original-double or an original-float.
        // 0x00 to 0x51 for the number of digits to right of the decimal point.
        if (type.isFractional() && decimals >= 0 && decimals <= 0x51) {
            return decimals;
        }

        return null;
    }

    @Override
    public CharCollation getCharCollation(CodecContext context) {
        return collationId == CharCollation.BINARY_ID ? context.getClientCollation() :
            CharCollation.fromId(collationId, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlColumnDescriptor)) {
            return false;
        }

        MySqlColumnDescriptor that = (MySqlColumnDescriptor) o;

        return index == that.index && size == that.size && decimals == that.decimals &&
            collationId == that.collationId &&
            typeMetadata.equals(that.typeMetadata) &&
            type == that.type && name.equals(that.name) &&
            nullability == that.nullability;
    }

    @Override
    public int hashCode() {
        int hash = 31 * index + typeMetadata.hashCode();
        hash = 31 * hash + type.hashCode();
        hash = 31 * hash + name.hashCode();
        hash = 31 * hash + nullability.hashCode();
        hash = 31 * hash + (int) (size ^ (size >>> 32));
        hash = 31 * hash + decimals;
        return 31 * hash + collationId;
    }

    @Override
    public String toString() {
        return "MySqlColumnDescriptor{index=" + index + ", typeMetadata=" + typeMetadata + ", type=" + type +
            ", name='" + name + "', nullability=" + nullability + ", size=" + size +
            ", decimals=" + decimals + ", collationId=" + collationId + '}';
    }
}
