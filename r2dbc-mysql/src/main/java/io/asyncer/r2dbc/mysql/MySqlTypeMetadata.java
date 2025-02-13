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

import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import io.asyncer.r2dbc.mysql.api.MySqlNativeTypeMetadata;
import io.asyncer.r2dbc.mysql.collation.CharCollation;

/**
 * An implementation of {@link MySqlNativeTypeMetadata}.
 */
final class MySqlTypeMetadata implements MySqlNativeTypeMetadata {

    private static final short NOT_NULL = 1;

//    public static final short PRIMARY_PART = 1 << 1; // This field is a part of the primary key
//    public static final short UNIQUE_PART = 1 << 2; // This field is a part of a unique key
//    public static final short KEY_PART = 1 << 3; // This field is a part of a normal key
//    public static final short BLOB = 1 << 4;

    private static final short UNSIGNED = 1 << 5;

//    public static final short ZEROFILL = 1 << 6;

    public static final short BINARY = 1 << 7;

    private static final short ENUM = 1 << 8;

//    public static final short AUTO_INCREMENT = 1 << 9;
//    public static final short TIMESTAMP = 1 << 10;

    private static final short SET = 1 << 11; // type is set

//    public static final short NO_DEFAULT = 1 << 12; // column has no default value
//    public static final short ON_UPDATE_NOW = 1 << 13; // field will be set to NOW() in UPDATE statement

    private static final short ALL_USED = NOT_NULL | UNSIGNED | BINARY | ENUM | SET;

    private final int typeId;

    /**
     * The original bitmap of definitions.
     * <p>
     * MySQL uses 32-bits definition flags, but only returns the lower 16-bits.
     */
    private final short definitions;

    /**
     * The character collation id of the column.
     * <p>
     * collationId > 0 when protocol version == 4.1, 0 otherwise.
     */
    private final int collationId;

    /**
     * The MariaDB extended type info field that provides more specific details about column type.
     */
    @Nullable
    private final String extendedTypeInfo;

    MySqlTypeMetadata(int typeId, int definitions, int collationId, @Nullable String extendedTypeInfo) {
        this.typeId = typeId;
        this.definitions = (short) (definitions & ALL_USED);
        this.collationId = collationId;
        this.extendedTypeInfo = extendedTypeInfo;
    }

    @Override
    public int getTypeId() {
        return typeId;
    }

    @Override
    public boolean isNotNull() {
        return (definitions & NOT_NULL) != 0;
    }

    @Override
    public boolean isUnsigned() {
        return (definitions & UNSIGNED) != 0;
    }

    @Override
    public boolean isBinary() {
        // Utilize collationId to ascertain whether it is binary or not.
        // This is necessary since the union of JSON columns, varchar binary, and char binary
        // results in a bitmap with the BINARY flag set.
        // see: https://github.com/asyncer-io/r2dbc-mysql/issues/91
        // FIXME: use collationId to check, definitions is not reliable even in protocol version < 4.1
        return (collationId == 0 && (definitions & BINARY) != 0) || collationId == CharCollation.BINARY_ID;
    }

    @Override
    public boolean isEnum() {
        return (definitions & ENUM) != 0;
    }

    @Override
    public boolean isSet() {
        return (definitions & SET) != 0;
    }

    @Override
    public boolean isMariaDbJson() {
        return (extendedTypeInfo == null ? false : extendedTypeInfo.equals("json"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlTypeMetadata)) {
            return false;
        }

        MySqlTypeMetadata that = (MySqlTypeMetadata) o;

        return typeId == that.typeId && definitions == that.definitions && collationId == that.collationId &&
                   Objects.equals(extendedTypeInfo, that.extendedTypeInfo);
    }

    @Override
    public int hashCode() {
        int result = 31 * typeId + (int) definitions;
        result = 31 * result + collationId;
        return 31 * result + (extendedTypeInfo == null ? 0 : extendedTypeInfo.hashCode());
    }

    @Override
    public String toString() {
        return "MySqlTypeMetadata{typeId=" + typeId +
            ", definitions=0x" + Integer.toHexString(definitions) +
            ", collationId=" + collationId +
            ", extendedTypeInfo='" + extendedTypeInfo +
            "'}";
    }
}
