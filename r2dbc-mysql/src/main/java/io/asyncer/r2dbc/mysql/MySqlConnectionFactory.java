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

import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final MySqlConnectionConfiguration configuration;
    private final LazyQueryCache queryCache;

    private MySqlConnectionFactory(MySqlConnectionConfiguration configuration) {
        this.configuration = configuration;
        this.queryCache = new LazyQueryCache(configuration.getQueryCacheSize());
    }

    @Override
    public Mono<? extends MySqlConnection> create() {
        MySqlSslConfiguration ssl;
        SocketAddress address;

        if (configuration.isHost()) {
            ssl = configuration.getSsl();
            address = InetSocketAddress.createUnresolved(configuration.getDomain(),
                    configuration.getPort());
        } else {
            ssl = MySqlSslConfiguration.disabled();
            address = new DomainSocketAddress(configuration.getDomain());
        }

        String user = configuration.getUser();
        CharSequence password = configuration.getPassword();
        Publisher<String> passwordPublisher = configuration.getPasswordPublisher();

        if (Objects.nonNull(passwordPublisher)) {
            return Mono.from(passwordPublisher).flatMap(token -> getMySqlConnection(
                    configuration, ssl,
                    queryCache,
                    address,
                    user,
                    token
            ));
        }

        return getMySqlConnection(
                configuration, ssl,
                queryCache,
                address,
                user,
                password
        );
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    /**
     * Creates a {@link MySqlConnectionFactory} with a {@link MySqlConnectionConfiguration}.
     *
     * @param configuration the {@link MySqlConnectionConfiguration}.
     * @return configured {@link MySqlConnectionFactory}.
     */
    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");
        return new MySqlConnectionFactory(configuration);
    }

    /**
     * Gets an initialized {@link MySqlConnection} from authentication credential and configurations.
     * <p>
     * It contains following steps:
     * <ol><li>Create connection context</li>
     * <li>Connect to MySQL server with TCP or Unix Domain Socket</li>
     * <li>Handshake/login and init handshake states</li>
     * <li>Init session states</li></ol>
     *
     * @param configuration the connection configuration.
     * @param ssl           the SSL configuration.
     * @param queryCache    lazy-init query cache, it is shared among all connections from the same factory.
     * @param address       TCP or Unix Domain Socket address.
     * @param user          the user of the authentication.
     * @param password      the password of the authentication.
     * @return a {@link MySqlConnection}.
     */
    private static Mono<MySqlConnection> getMySqlConnection(
        final MySqlConnectionConfiguration configuration,
        final MySqlSslConfiguration ssl,
        final LazyQueryCache queryCache,
        final SocketAddress address,
        final String user,
        @Nullable final CharSequence password
    ) {
        return Mono.fromSupplier(() -> {
            ZoneId connectionTimeZone = retrieveZoneId(configuration.getConnectionTimeZone());
            return new ConnectionContext(
                configuration.getZeroDateOption(),
                configuration.getLoadLocalInfilePath(),
                configuration.getLocalInfileBufferSize(),
                configuration.isTinyInt1isBit(),
                configuration.isPreserveInstants(),
                connectionTimeZone
            );
        }).flatMap(context -> Client.connect(
            ssl,
            address,
            configuration.isTcpKeepAlive(),
            configuration.isTcpNoDelay(),
            context,
            configuration.getConnectTimeout(),
            configuration.getLoopResources(),
            configuration.getResolver(),
            configuration.isMetrics()
        )).flatMap(client -> {
            // Lazy init database after handshake/login
            boolean deferDatabase = configuration.isCreateDatabaseIfNotExist();
            String database = configuration.getDatabase();
            String loginDb = deferDatabase ? "" : database;
            String sessionDb = deferDatabase ? database : "";

            return InitFlow.initHandshake(
                client,
                ssl.getSslMode(),
                loginDb,
                user,
                password,
                configuration.getCompressionAlgorithms(),
                configuration.getZstdCompressionLevel()
            ).then(InitFlow.initSession(
                client,
                sessionDb,
                configuration.getPrepareCacheSize(),
                configuration.getSessionVariables(),
                configuration.isForceConnectionTimeZoneToSession(),
                configuration.getLockWaitTimeout(),
                configuration.getStatementTimeout(),
                configuration.getExtensions()
            )).map(codecs -> new MySqlSimpleConnection(
                client,
                codecs,
                queryCache.get(),
                configuration.getPreferPrepareStatement()
            )).onErrorResume(e -> client.forceClose().then(Mono.error(e)));
        });
    }

    @Nullable
    private static ZoneId retrieveZoneId(String timeZone) {
        if ("LOCAL".equalsIgnoreCase(timeZone)) {
            return ZoneId.systemDefault().normalized();
        } else if ("SERVER".equalsIgnoreCase(timeZone)) {
            return null;
        }

        return StringUtils.parseZoneId(timeZone);
    }

    private static final class LazyQueryCache implements Supplier<QueryCache> {

        private final int capacity;

        private final ReentrantLock lock = new ReentrantLock();

        @Nullable
        private volatile QueryCache cache;

        private LazyQueryCache(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public QueryCache get() {
            QueryCache cache = this.cache;
            if (cache == null) {
                lock.lock();
                try {
                    if ((cache = this.cache) == null) {
                        this.cache = cache = Caches.createQueryCache(capacity);
                    }
                    return cache;
                } finally {
                    lock.unlock();
                }
            }
            return cache;
        }
    }
}
