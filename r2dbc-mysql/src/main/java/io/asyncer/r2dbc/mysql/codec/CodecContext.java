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

package io.asyncer.r2dbc.mysql.codec;

import io.asyncer.r2dbc.mysql.ServerVersion;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;

import java.time.ZoneId;

/**
 * Codec variables context for encoding/decoding.
 */
public interface CodecContext {

    /**
     * Checks if the connection is set to preserve instants, i.e. convert instant values to connection time
     * zone.
     *
     * @return if preserve instants.
     */
    boolean isPreserveInstants();

    /**
     * Gets the {@link ZoneId} of connection.
     *
     * @return the {@link ZoneId}.
     */
    ZoneId getTimeZone();

    /**
     * Gets the option for zero date handling which is set by connection configuration.
     *
     * @return the {@link ZeroDateOption}.
     */
    ZeroDateOption getZeroDateOption();

    /**
     * Gets the MySQL server version, which is available after database user logon.
     *
     * @return the {@link ServerVersion}.
     */
    ServerVersion getServerVersion();

    /**
     * Gets the {@link CharCollation} that the client is using.
     *
     * @return the {@link CharCollation}.
     */
    CharCollation getClientCollation();

    /**
     * Checks server is MariaDB or not.
     *
     * @return if is MariaDB.
     */
    boolean isMariaDb();

    /**
     *
     * @return true if tinyInt(1) is treated as bit.
     */
    boolean isTinyInt1isBit();
}
