/*
 * Copyright 2024 asyncer.io projects
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

package io.asyncer.r2dbc.mysql.api;

/**
 * An interface for MySQL native type metadata.
 *
 * @see MySqlReadableMetadata#getNativeTypeMetadata()
 * @since 1.1.3
 */
public interface MySqlNativeTypeMetadata {

    /**
     * Gets the native type identifier, e.g. {@code 3} for {@code INT}.
     * <p>
     * Note: It can not check if the current type is unsigned or not, and some types will use the same
     * identifier. e.g. {@code TEXT} and {@code BLOB} are using {@code 252}.
     *
     * @return the native type identifier
     */
    int getTypeId();

    /**
     * Checks if the value is not null.
     *
     * @return if value is not null
     */
    boolean isNotNull();

    /**
     * Checks if the value is an unsigned number. e.g. INT UNSIGNED, BIGINT UNSIGNED.
     * <p>
     * Note: IEEE-754 floating types (e.g. DOUBLE/FLOAT) do not support it in MySQL 8.0+. When creating a
     * column as an unsigned floating type, the server may report a warning.
     *
     * @return if value is an unsigned number
     */
    boolean isUnsigned();

    /**
     * Checks if the value is binary data.
     *
     * @return if value is binary data
     */
    boolean isBinary();

    /**
     * Checks if the value type is enum.
     *
     * @return if value is an enum
     */
    boolean isEnum();

    /**
     * Checks if the value type is set.
     *
     * @return if value is a set
     */
    boolean isSet();

    /**
     * Checks if value is JSON for MariaDb.
     *
     * @return if value is a JSON for MariaDb
     */
    boolean isMariaDbJson();
}
