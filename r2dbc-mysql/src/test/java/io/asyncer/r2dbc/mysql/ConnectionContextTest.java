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

import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link ConnectionContext}.
 */
public class ConnectionContextTest {

    @Test
    void getTimeZone() {
        for (int i = -12; i <= 12; ++i) {
            String id = i < 0 ? "UTC" + i : "UTC+" + i;
            ConnectionContext context = new ConnectionContext(
                ZeroDateOption.USE_NULL, null,
                8192, true, true, ZoneId.of(id));

            assertThat(context.getTimeZone()).isEqualTo(ZoneId.of(id));
        }
    }

    @Test
    void setTwiceTimeZone() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, true, true, null);

        context.initSession(
            Caches.createPrepareCache(0),
            IsolationLevel.REPEATABLE_READ,
            false, Duration.ZERO,
            null,
            ZoneId.systemDefault()
        );
        assertThatIllegalStateException().isThrownBy(() -> context.initSession(
            Caches.createPrepareCache(0),
            IsolationLevel.REPEATABLE_READ,
            false,
            Duration.ZERO,
            null,
            ZoneId.systemDefault()
        ));
    }

    @Test
    void badSetTimeZone() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, true, true, ZoneId.systemDefault());
        assertThatIllegalStateException().isThrownBy(() -> context.initSession(
            Caches.createPrepareCache(0),
            IsolationLevel.REPEATABLE_READ,
            false,
            Duration.ZERO,
            null,
            ZoneId.systemDefault()
        ));
    }

    public static ConnectionContext mock() {
        return mock(false, ZoneId.systemDefault());
    }

    public static ConnectionContext mock(boolean isMariaDB) {
        return mock(isMariaDB, ZoneId.systemDefault());
    }

    public static ConnectionContext mock(boolean isMariaDB, ZoneId zoneId) {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, true, true, zoneId);

        context.initHandshake(1, ServerVersion.parse(isMariaDB ? "11.2.22.MOCKED" : "8.0.11.MOCKED"),
            Capability.of(~(isMariaDB ? 1 : 0)));
        context.setServerStatuses(ServerStatuses.AUTO_COMMIT);

        return context;
    }
}
