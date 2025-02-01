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

import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.asyncer.r2dbc.mysql.extension.Extension;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.AddressResolverGroup;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.netty.internal.util.Metrics;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;

import javax.net.ssl.HostnameVerifier;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;
import static io.asyncer.r2dbc.mysql.internal.util.InternalArrays.EMPTY_STRINGS;

/**
 * A configuration of MySQL connection.
 */
public final class MySqlConnectionConfiguration {

    /**
     * Default MySQL port.
     */
    private static final int DEFAULT_PORT = 3306;

    /**
     * {@code true} if {@link #domain} is hostname, otherwise {@link #domain} is unix domain socket path.
     */
    private final boolean isHost;

    /**
     * Domain of connecting, may be hostname or unix domain socket path.
     */
    private final String domain;

    private final int port;

    private final MySqlSslConfiguration ssl;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    @Nullable
    private final Duration connectTimeout;

    private final boolean preserveInstants;

    private final String connectionTimeZone;

    private final boolean forceConnectionTimeZoneToSession;

    private final ZeroDateOption zeroDateOption;

    private final String user;

    @Nullable
    private final CharSequence password;

    private final String database;

    private final boolean createDatabaseIfNotExist;

    @Nullable
    private final Predicate<String> preferPrepareStatement;

    private final List<String> sessionVariables;

    @Nullable
    private final Duration lockWaitTimeout;

    @Nullable
    private final Duration statementTimeout;

    @Nullable
    private final Path loadLocalInfilePath;

    private final int localInfileBufferSize;

    private final int queryCacheSize;

    private final int prepareCacheSize;

    private final Set<CompressionAlgorithm> compressionAlgorithms;

    private final int zstdCompressionLevel;

    private final LoopResources loopResources;

    private final Extensions extensions;

    @Nullable
    private final Publisher<String> passwordPublisher;

    @Nullable
    private final AddressResolverGroup<?> resolver;

    private final boolean metrics;

    private final boolean tinyInt1isBit;

    private MySqlConnectionConfiguration(
            boolean isHost, String domain, int port, MySqlSslConfiguration ssl,
            boolean tcpKeepAlive, boolean tcpNoDelay, @Nullable Duration connectTimeout,
            ZeroDateOption zeroDateOption,
            boolean preserveInstants,
            String connectionTimeZone,
            boolean forceConnectionTimeZoneToSession,
            String user, @Nullable CharSequence password, @Nullable String database,
            boolean createDatabaseIfNotExist, @Nullable Predicate<String> preferPrepareStatement,
            List<String> sessionVariables, @Nullable Duration lockWaitTimeout, @Nullable Duration statementTimeout,
            @Nullable Path loadLocalInfilePath, int localInfileBufferSize,
            int queryCacheSize, int prepareCacheSize,
            Set<CompressionAlgorithm> compressionAlgorithms, int zstdCompressionLevel,
            @Nullable LoopResources loopResources,
            Extensions extensions, @Nullable Publisher<String> passwordPublisher,
            @Nullable AddressResolverGroup<?> resolver,
            boolean metrics,
            boolean tinyInt1isBit) {
        this.isHost = isHost;
        this.domain = domain;
        this.port = port;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.connectTimeout = connectTimeout;
        this.ssl = ssl;
        this.preserveInstants = preserveInstants;
        this.connectionTimeZone = requireNonNull(connectionTimeZone, "connectionTimeZone must not be null");
        this.forceConnectionTimeZoneToSession = forceConnectionTimeZoneToSession;
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.user = requireNonNull(user, "user must not be null");
        this.password = password;
        this.database = database == null || database.isEmpty() ? "" : database;
        this.createDatabaseIfNotExist = createDatabaseIfNotExist;
        this.preferPrepareStatement = preferPrepareStatement;
        this.sessionVariables = sessionVariables;
        this.lockWaitTimeout = lockWaitTimeout;
        this.statementTimeout = statementTimeout;
        this.loadLocalInfilePath = loadLocalInfilePath;
        this.localInfileBufferSize = localInfileBufferSize;
        this.queryCacheSize = queryCacheSize;
        this.prepareCacheSize = prepareCacheSize;
        this.compressionAlgorithms = compressionAlgorithms;
        this.zstdCompressionLevel = zstdCompressionLevel;
        this.loopResources = loopResources == null ? TcpResources.get() : loopResources;
        this.extensions = extensions;
        this.passwordPublisher = passwordPublisher;
        this.resolver = resolver;
        this.metrics = metrics;
        this.tinyInt1isBit = tinyInt1isBit;
    }

    /**
     * Creates a builder of the configuration. All options are default.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    boolean isHost() {
        return isHost;
    }

    String getDomain() {
        return domain;
    }

    int getPort() {
        return port;
    }

    @Nullable
    Duration getConnectTimeout() {
        return connectTimeout;
    }

    MySqlSslConfiguration getSsl() {
        return ssl;
    }

    boolean isTcpKeepAlive() {
        return this.tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return this.tcpNoDelay;
    }

    ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    boolean isPreserveInstants() {
        return preserveInstants;
    }

    String getConnectionTimeZone() {
        return connectionTimeZone;
    }

    boolean isForceConnectionTimeZoneToSession() {
        return forceConnectionTimeZoneToSession;
    }

    String getUser() {
        return user;
    }

    @Nullable
    CharSequence getPassword() {
        return password;
    }

    String getDatabase() {
        return database;
    }

    boolean isCreateDatabaseIfNotExist() {
        return createDatabaseIfNotExist;
    }

    @Nullable
    Predicate<String> getPreferPrepareStatement() {
        return preferPrepareStatement;
    }

    List<String> getSessionVariables() {
        return sessionVariables;
    }

    @Nullable
    Duration getLockWaitTimeout() {
        return lockWaitTimeout;
    }

    @Nullable
    Duration getStatementTimeout() {
        return statementTimeout;
    }

    @Nullable
    Path getLoadLocalInfilePath() {
        return loadLocalInfilePath;
    }

    int getLocalInfileBufferSize() {
        return localInfileBufferSize;
    }

    int getQueryCacheSize() {
        return queryCacheSize;
    }

    int getPrepareCacheSize() {
        return prepareCacheSize;
    }

    Set<CompressionAlgorithm> getCompressionAlgorithms() {
        return compressionAlgorithms;
    }

    int getZstdCompressionLevel() {
        return zstdCompressionLevel;
    }

    LoopResources getLoopResources() {
        return loopResources;
    }

    Extensions getExtensions() {
        return extensions;
    }

    @Nullable
    Publisher<String> getPasswordPublisher() {
        return passwordPublisher;
    }

    @Nullable
    AddressResolverGroup<?> getResolver() {
        return resolver;
    }

    boolean isMetrics() {
        return metrics;
    }

    boolean isTinyInt1isBit() {
        return tinyInt1isBit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlConnectionConfiguration)) {
            return false;
        }
        MySqlConnectionConfiguration that = (MySqlConnectionConfiguration) o;
        return isHost == that.isHost &&
            domain.equals(that.domain) &&
            port == that.port &&
            ssl.equals(that.ssl) &&
            tcpKeepAlive == that.tcpKeepAlive &&
            tcpNoDelay == that.tcpNoDelay &&
            Objects.equals(connectTimeout, that.connectTimeout) &&
            preserveInstants == that.preserveInstants &&
            Objects.equals(connectionTimeZone, that.connectionTimeZone) &&
            forceConnectionTimeZoneToSession == that.forceConnectionTimeZoneToSession &&
            zeroDateOption == that.zeroDateOption &&
            user.equals(that.user) &&
            Objects.equals(password, that.password) &&
            database.equals(that.database) &&
            createDatabaseIfNotExist == that.createDatabaseIfNotExist &&
            Objects.equals(preferPrepareStatement, that.preferPrepareStatement) &&
            sessionVariables.equals(that.sessionVariables) &&
            Objects.equals(lockWaitTimeout, that.lockWaitTimeout) &&
            Objects.equals(statementTimeout, that.statementTimeout) &&
            Objects.equals(loadLocalInfilePath, that.loadLocalInfilePath) &&
            localInfileBufferSize == that.localInfileBufferSize &&
            queryCacheSize == that.queryCacheSize &&
            prepareCacheSize == that.prepareCacheSize &&
            compressionAlgorithms.equals(that.compressionAlgorithms) &&
            zstdCompressionLevel == that.zstdCompressionLevel &&
            Objects.equals(loopResources, that.loopResources) &&
            extensions.equals(that.extensions) &&
            Objects.equals(passwordPublisher, that.passwordPublisher) &&
            Objects.equals(resolver, that.resolver) &&
            metrics == that.metrics &&
            tinyInt1isBit == that.tinyInt1isBit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isHost, domain, port, ssl, tcpKeepAlive, tcpNoDelay, connectTimeout,
            preserveInstants, connectionTimeZone, forceConnectionTimeZoneToSession,
            zeroDateOption, user, password, database, createDatabaseIfNotExist,
            preferPrepareStatement,
            sessionVariables,
            lockWaitTimeout,
            statementTimeout,
            loadLocalInfilePath, localInfileBufferSize,
            queryCacheSize, prepareCacheSize,
            compressionAlgorithms, zstdCompressionLevel,
            loopResources, extensions, passwordPublisher, resolver, metrics, tinyInt1isBit);
    }

    @Override
    public String toString() {
        return "MySqlConnectionConfiguration{" +
                (isHost ? "host='" + domain + "', port=" + port + ", ssl=" + ssl +
                          ", tcpNoDelay=" + tcpNoDelay + ", tcpKeepAlive=" + tcpKeepAlive :
                        "unixSocket='" + domain + "'") +
                buildCommonToStringPart() +
                '}';
    }

    private String buildCommonToStringPart() {
        return ", connectTimeout=" + connectTimeout +
                ", preserveInstants=" + preserveInstants +
                ", connectionTimeZone=" + connectionTimeZone +
                ", forceConnectionTimeZoneToSession=" + forceConnectionTimeZoneToSession +
                ", zeroDateOption=" + zeroDateOption +
                ", user='" + user + "', password=" + password +
                ", database='" + database + "', createDatabaseIfNotExist=" + createDatabaseIfNotExist +
                ", preferPrepareStatement=" + preferPrepareStatement +
                ", sessionVariables=" + sessionVariables +
                ", lockWaitTimeout=" + lockWaitTimeout +
                ", statementTimeout=" + statementTimeout +
                ", loadLocalInfilePath=" + loadLocalInfilePath +
                ", localInfileBufferSize=" + localInfileBufferSize +
                ", queryCacheSize=" + queryCacheSize +
                ", prepareCacheSize=" + prepareCacheSize +
                ", compressionAlgorithms=" + compressionAlgorithms +
                ", zstdCompressionLevel=" + zstdCompressionLevel +
                ", loopResources=" + loopResources +
                ", extensions=" + extensions +
                ", passwordPublisher=" + passwordPublisher +
                ", resolver=" + resolver +
                ", metrics=" + metrics +
                ", tinyint1isBit=" + tinyInt1isBit;
    }

    /**
     * A builder for {@link MySqlConnectionConfiguration} creation.
     */
    public static final class Builder {

        @Nullable
        private String database;

        private boolean createDatabaseIfNotExist;

        private boolean isHost = true;

        private String domain;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        @Nullable
        private Duration connectTimeout;

        private String user;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        private boolean preserveInstants = true;

        private String connectionTimeZone = "LOCAL";

        private boolean forceConnectionTimeZoneToSession;

        @Nullable
        private SslMode sslMode;

        private String[] tlsVersion = EMPTY_STRINGS;

        @Nullable
        private HostnameVerifier sslHostnameVerifier;

        @Nullable
        private String sslCa;

        @Nullable
        private String sslKey;

        @Nullable
        private CharSequence sslKeyPassword;

        @Nullable
        private String sslCert;

        @Nullable
        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

        private boolean tcpKeepAlive;

        private boolean tcpNoDelay;

        @Nullable
        private Predicate<String> preferPrepareStatement;

        @Nullable
        private Duration lockWaitTimeout;

        @Nullable
        private Duration statementTimeout;

        private List<String> sessionVariables = Collections.emptyList();

        @Nullable
        private Path loadLocalInfilePath;

        private int localInfileBufferSize = 8192;

        private int queryCacheSize = 0;

        private int prepareCacheSize = 256;

        private Set<CompressionAlgorithm> compressionAlgorithms =
            Collections.singleton(CompressionAlgorithm.UNCOMPRESSED);

        private int zstdCompressionLevel = 3;

        @Nullable
        private LoopResources loopResources;

        private boolean autodetectExtensions = true;

        private final List<Extension> extensions = new ArrayList<>();

        @Nullable
        private Publisher<String> passwordPublisher;

        @Nullable
        private AddressResolverGroup<?> resolver;

        private boolean metrics;

        private boolean tinyInt1isBit = true;

        /**
         * Builds an immutable {@link MySqlConnectionConfiguration} with current options.
         *
         * @return the {@link MySqlConnectionConfiguration}.
         */
        public MySqlConnectionConfiguration build() {
            SslMode sslMode = requireSslMode();

            if (isHost) {
                requireNonNull(domain, "host must not be null when using TCP socket");
                require((sslCert == null && sslKey == null) || (sslCert != null && sslKey != null),
                    "sslCert and sslKey must be both null or both non-null");
            } else {
                requireNonNull(domain, "unixSocket must not be null when using unix domain socket");
                require(!sslMode.startSsl(), "sslMode must be disabled when using unix domain socket");
            }

            int prepareCacheSize = preferPrepareStatement == null ? 0 : this.prepareCacheSize;

            MySqlSslConfiguration ssl = MySqlSslConfiguration.create(sslMode, tlsVersion, sslHostnameVerifier,
                sslCa, sslKey, sslKeyPassword, sslCert, sslContextBuilderCustomizer);
            return new MySqlConnectionConfiguration(isHost, domain, port, ssl, tcpKeepAlive, tcpNoDelay,
                connectTimeout, zeroDateOption,
                preserveInstants,
                connectionTimeZone,
                forceConnectionTimeZoneToSession,
                user, password, database,
                createDatabaseIfNotExist, preferPrepareStatement,
                sessionVariables,
                lockWaitTimeout,
                statementTimeout,
                loadLocalInfilePath,
                localInfileBufferSize, queryCacheSize, prepareCacheSize,
                compressionAlgorithms, zstdCompressionLevel, loopResources,
                Extensions.from(extensions, autodetectExtensions), passwordPublisher, resolver, metrics, tinyInt1isBit);
        }

        /**
         * Configures the database. Default no database.
         *
         * @param database the database, or {@code null} if no database want to be login.
         * @return this {@link Builder}.
         * @since 0.8.1
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Configures to create the database given in the configuration if it does not yet exist.  Default to
         * {@code false}.
         *
         * @param enabled to discover and register extensions.
         * @return this {@link Builder}.
         * @since 1.0.6
         */
        public Builder createDatabaseIfNotExist(boolean enabled) {
            this.createDatabaseIfNotExist = enabled;
            return this;
        }

        /**
         * Configures the Unix Domain Socket to connect to.
         *
         * @param unixSocket the socket file path.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code unixSocket} is {@code null}.
         * @since 0.8.1
         */
        public Builder unixSocket(String unixSocket) {
            this.domain = requireNonNull(unixSocket, "unixSocket must not be null");
            this.isHost = false;
            return this;
        }

        /**
         * Configures the host.
         *
         * @param host the host.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code host} is {@code null}.
         * @since 0.8.1
         */
        public Builder host(String host) {
            this.domain = requireNonNull(host, "host must not be null");
            this.isHost = true;
            return this;
        }

        /**
         * Configures the password.  Default login without password.
         * <p>
         * Note: for memory security, should not use intern {@link String} for password.
         *
         * @param password the password, or {@code null} when user has no password.
         * @return this {@link Builder}.
         * @since 0.8.1
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        /**
         * Configures the port.  Defaults to {@code 3306}.
         *
         * @param port the port.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if the {@code port} is negative or bigger than {@literal 65535}.
         * @since 0.8.1
         */
        public Builder port(int port) {
            require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535");

            this.port = port;
            return this;
        }

        /**
         * Configures the connection timeout.  Default no timeout.
         *
         * @param connectTimeout the connection timeout, or {@code null} if no timeout.
         * @return this {@link Builder}.
         * @since 0.8.1
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Configures the user for login the database.
         *
         * @param user the user.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code user} is {@code null}.
         * @since 0.8.2
         */
        public Builder user(String user) {
            this.user = requireNonNull(user, "user must not be null");
            return this;
        }

        /**
         * An alias of {@link #user(String)}.
         *
         * @param user the user.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code user} is {@code null}.
         * @since 0.8.1
         */
        public Builder username(String user) {
            return user(user);
        }

        /**
         * Configures the time zone conversion.  Default to {@code true} means enable conversion between JVM
         * and {@link #connectionTimeZone(String)}.
         * <p>
         * Note: disable it will ignore the time zone of connection, and use the JVM local time zone.
         *
         * @param enabled {@code true} to preserve instants, or {@code false} to disable conversion.
         * @return {@link Builder this}
         * @since 1.1.2
         */
        public Builder preserveInstants(boolean enabled) {
            this.preserveInstants = enabled;
            return this;
        }

        /**
         * Configures the time zone of connection.  Default to {@code LOCAL} means use JVM local time zone.
         * {@code "SERVER"} means querying the server-side timezone during initialization.
         *
         * @param connectionTimeZone {@code "LOCAL"}, {@code "SERVER"}, or a valid ID of {@code ZoneId}.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code connectionTimeZone} is {@code null} or empty.
         * @since 1.1.2
         */
        public Builder connectionTimeZone(String connectionTimeZone) {
            requireNonEmpty(connectionTimeZone, "connectionTimeZone must not be empty");

            this.connectionTimeZone = connectionTimeZone;
            return this;
        }

        /**
         * Configures to force the connection time zone to session time zone.  Default to {@code false}. Used
         * only if the {@link #connectionTimeZone(String)} is not {@code "SERVER"}.
         * <p>
         * Note: alter the time zone of session will affect the results of MySQL date/time functions, e.g.
         * {@code NOW([n])}, {@code CURRENT_TIME([n])}, {@code CURRENT_DATE()}, etc. Please use with caution.
         *
         * @param enabled {@code true} to force the connection time zone to session time zone.
         * @return {@link Builder this}
         * @since 1.1.2
         */
        public Builder forceConnectionTimeZoneToSession(boolean enabled) {
            this.forceConnectionTimeZoneToSession = enabled;
            return this;
        }

        /**
         * Configures the time zone of server.  Since 1.1.2, default to use JVM local time zone.
         *
         * @param serverZoneId the {@link ZoneId}, or {@code null} if query server during initialization.
         * @return {@link Builder this}
         * @since 0.8.2
         * @deprecated since 1.1.2, use {@link #connectionTimeZone(String)} instead.
         */
        @Deprecated
        public Builder serverZoneId(@Nullable ZoneId serverZoneId) {
            return connectionTimeZone(serverZoneId == null ? "SERVER" : serverZoneId.getId());
        }

        /**
         * Configures the {@link ZeroDateOption}.  Default to {@link ZeroDateOption#USE_NULL}.  It is a
         * behavior option when this driver receives a value of zero-date.
         *
         * @param zeroDate the {@link ZeroDateOption}.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code zeroDate} is {@code null}.
         * @since 0.8.1
         */
        public Builder zeroDateOption(ZeroDateOption zeroDate) {
            this.zeroDateOption = requireNonNull(zeroDate, "zeroDateOption must not be null");
            return this;
        }

        /**
         * Configures ssl mode.  See also {@link SslMode}.
         *
         * @param sslMode the SSL mode to use.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code sslMode} is {@code null}.
         * @since 0.8.1
         */
        public Builder sslMode(SslMode sslMode) {
            this.sslMode = requireNonNull(sslMode, "sslMode must not be null");
            return this;
        }

        /**
         * Configures TLS versions, see {@link io.asyncer.r2dbc.mysql.constant.TlsVersions TlsVersions}.
         *
         * @param tlsVersion TLS versions.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if the array {@code tlsVersion} is {@code null}.
         * @since 0.8.1
         */
        public Builder tlsVersion(String... tlsVersion) {
            requireNonNull(tlsVersion, "tlsVersion must not be null");

            int size = tlsVersion.length;

            if (size > 0) {
                String[] versions = new String[size];
                System.arraycopy(tlsVersion, 0, versions, 0, size);
                this.tlsVersion = versions;
            } else {
                this.tlsVersion = EMPTY_STRINGS;
            }
            return this;
        }

        /**
         * Configures SSL {@link HostnameVerifier}, it is available only set {@link #sslMode(SslMode)} as
         * {@link SslMode#VERIFY_IDENTITY}. It is useful when server was using special Certificates or need
         * special verification.
         * <p>
         * Default is builtin {@link HostnameVerifier} which use RFC standards.
         *
         * @param sslHostnameVerifier the custom {@link HostnameVerifier}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslHostnameVerifier} is {@code null}
         * @since 0.8.2
         */
        public Builder sslHostnameVerifier(HostnameVerifier sslHostnameVerifier) {
            this.sslHostnameVerifier = requireNonNull(sslHostnameVerifier,
                "sslHostnameVerifier must not be null");
            return this;
        }

        /**
         * Configures SSL root certification for server certificate validation. It is only available if the
         * {@link #sslMode(SslMode)} is configured for verify server certification.
         * <p>
         * Default is {@code null}, which means that the default algorithm is used for the trust manager.
         *
         * @param sslCa an X.509 certificate chain file in PEM format.
         * @return this {@link Builder}.
         * @since 0.8.1
         */
        public Builder sslCa(@Nullable String sslCa) {
            this.sslCa = sslCa;
            return this;
        }

        /**
         * Configures client SSL certificate for client authentication.
         * <p>
         * The {@link #sslCert} and {@link #sslKey} must be both non-{@code null} or both {@code null}.
         *
         * @param sslCert an X.509 certificate chain file in PEM format, or {@code null} if no SSL cert.
         * @return this {@link Builder}.
         * @since 0.8.2
         */
        public Builder sslCert(@Nullable String sslCert) {
            this.sslCert = sslCert;
            return this;
        }

        /**
         * Configures client SSL key for client authentication.
         * <p>
         * The {@link #sslCert} and {@link #sslKey} must be both non-{@code null} or both {@code null}.
         *
         * @param sslKey a PKCS#8 private key file in PEM format, or {@code null} if no SSL key.
         * @return this {@link Builder}.
         * @since 0.8.2
         */
        public Builder sslKey(@Nullable String sslKey) {
            this.sslKey = sslKey;
            return this;
        }

        /**
         * Configures the password of SSL key file for client certificate authentication.
         * <p>
         * It will be used only if {@link #sslKey} and {@link #sslCert} non-null.
         *
         * @param sslKeyPassword the password of the {@link #sslKey}, or {@code null} if it's not
         *                       password-protected.
         * @return this {@link Builder}.
         * @since 0.8.2
         */
        public Builder sslKeyPassword(@Nullable CharSequence sslKeyPassword) {
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        /**
         * Configures a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL
         * connection attempt to allow for just-in-time configuration updates. The {@link Function} gets
         * called with the prepared {@link SslContextBuilder} that has all configuration options applied. The
         * customizer may return the same builder or return a new builder instance to be used to build the SSL
         * context.
         *
         * @param customizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code customizer} is {@code null}
         * @since 0.8.1
         */
        public Builder sslContextBuilderCustomizer(
            Function<SslContextBuilder, SslContextBuilder> customizer) {
            requireNonNull(customizer, "sslContextBuilderCustomizer must not be null");

            this.sslContextBuilderCustomizer = customizer;
            return this;
        }

        /**
         * Configures TCP KeepAlive.
         *
         * @param enabled whether to enable TCP KeepAlive
         * @return this {@link Builder}
         * @see Socket#setKeepAlive(boolean)
         * @since 0.8.2
         */
        public Builder tcpKeepAlive(boolean enabled) {
            this.tcpKeepAlive = enabled;
            return this;
        }

        /**
         * Configures TCP NoDelay.
         *
         * @param enabled whether to enable TCP NoDelay
         * @return this {@link Builder}
         * @see Socket#setTcpNoDelay(boolean)
         * @since 0.8.2
         */
        public Builder tcpNoDelay(boolean enabled) {
            this.tcpNoDelay = enabled;
            return this;
        }

        /**
         * Configures the protocol of parameterized statements to the text protocol.
         * <p>
         * The text protocol is default protocol that's using client-preparing. See also MySQL
         * documentations.
         *
         * @return this {@link Builder}
         * @since 0.8.1
         */
        public Builder useClientPrepareStatement() {
            this.preferPrepareStatement = null;
            return this;
        }

        /**
         * Configures the protocol of parameterized statements to the binary protocol.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing. See also MySQL
         * documentations.
         *
         * @return this {@link Builder}.
         * @since 0.8.1
         */
        public Builder useServerPrepareStatement() {
            return useServerPrepareStatement((sql) -> false);
        }

        /**
         * Configures the protocol of parameterized statements and prepare-preferred simple statements to the
         * binary protocol.
         * <p>
         * The {@code preferPrepareStatement} configures whether to prefer prepare execution on a
         * statement-by-statement basis (simple statements). The {@link Predicate} accepts the simple SQL
         * query string and returns a boolean flag indicating preference. {@code true} prepare-preferred,
         * {@code false} prefers direct execution (text protocol). Defaults to direct execution.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing. See also MySQL
         * documentations.
         *
         * @param preferPrepareStatement the above {@link Predicate}.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code preferPrepareStatement} is {@code null}.
         * @since 0.8.1
         */
        public Builder useServerPrepareStatement(Predicate<String> preferPrepareStatement) {
            requireNonNull(preferPrepareStatement, "preferPrepareStatement must not be null");

            this.preferPrepareStatement = preferPrepareStatement;
            return this;
        }

        /**
         * Configures the session variables, used to set session variables immediately after login. Default no
         * session variables to set.  It should be a list of key-value pairs. e.g.
         * {@code ["sql_mode='ANSI_QUOTES,STRICT_TRANS_TABLES'", "time_zone=00:00"]}.
         *
         * @param sessionVariables the session variables to set.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code sessionVariables} is {@code null}.
         * @since 1.1.2
         */
        public Builder sessionVariables(String... sessionVariables) {
            requireNonNull(sessionVariables, "sessionVariables must not be null");

            this.sessionVariables = InternalArrays.toImmutableList(sessionVariables);
            return this;
        }

        /**
         * Configures the lock wait timeout.  Default to use the server-side default value.
         *
         * @param lockWaitTimeout the lock wait timeout, or {@code null} to use the server-side default value.
         * @return {@link Builder this}
         * @since 1.1.3
         */
        public Builder lockWaitTimeout(@Nullable Duration lockWaitTimeout) {
            this.lockWaitTimeout = lockWaitTimeout;
            return this;
        }

        /**
         * Configures the statement timeout.  Default to use the server-side default value.
         *
         * @param statementTimeout the statement timeout, or {@code null} to use the server-side default value.
         * @return {@link Builder this}
         * @since 1.1.3
         */
        public Builder statementTimeout(@Nullable Duration statementTimeout) {
            this.statementTimeout = statementTimeout;
            return this;
        }

        /**
         * Configures to allow the {@code LOAD DATA LOCAL INFILE} statement in the given {@code path} or
         * disallow the statement.  Default to {@code null} which means not allow the statement.
         *
         * @param path which parent path are allowed to load file data, {@code null} means not be allowed.
         * @return {@link Builder this}.
         * @throws java.nio.file.InvalidPathException if the string cannot be converted to a {@link Path}.
         * @since 1.1.0
         */
        public Builder allowLoadLocalInfileInPath(@Nullable String path) {
            this.loadLocalInfilePath = path == null ? null : Paths.get(path);

            return this;
        }

        /**
         * Configures the buffer size for {@code LOAD DATA LOCAL INFILE} statement.  Default to {@code 8192}.
         * <p>
         * It is used only if {@link #allowLoadLocalInfileInPath(String)} is set.
         *
         * @param localInfileBufferSize the buffer size.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code localInfileBufferSize} is not positive.
         * @since 1.1.0
         */
        public Builder localInfileBufferSize(int localInfileBufferSize) {
            require(localInfileBufferSize > 0, "localInfileBufferSize must be positive");

            this.localInfileBufferSize = localInfileBufferSize;
            return this;
        }

        /**
         * Configures the maximum size of the {@link Query} parsing cache. Usually it should be power of two.
         * Default to {@code 0}. Driver will use unbounded cache if size is less than {@code 0}.
         * <p>
         * Notice: the cache is using EL model (the PACELC theorem) which provider better performance. That
         * means it is an elastic cache. So this size is not a hard-limit. It should be over 16 in average.
         *
         * @param queryCacheSize the above size, {@code 0} means no cache, {@code -1} means unbounded cache.
         * @return this {@link Builder}.
         * @since 0.8.3
         */
        public Builder queryCacheSize(int queryCacheSize) {
            this.queryCacheSize = queryCacheSize;
            return this;
        }

        /**
         * Configures the maximum size of the server-preparing cache. Usually it should be power of two.
         * Default to {@code 256}. Driver will use unbounded cache if size is less than {@code 0}. It is used
         * only if using server-preparing parameterized statements, i.e. the {@link #useServerPrepareStatement}
         * is set.
         * <p>
         * Notice: the cache is using EC model (the PACELC theorem) for ensure consistency. Consistency is
         * very important because MySQL contains a hard limit of all server-prepared statements which has been
         * opened, see also {@code max_prepared_stmt_count}. And, the cache is one-to-one connection, which
         * means it will not work on thread-concurrency.
         *
         * @param prepareCacheSize the above size, {@code 0} means no cache, {@code -1} means unbounded
         *                         cache.
         * @return this {@link Builder}.
         * @since 0.8.3
         */
        public Builder prepareCacheSize(int prepareCacheSize) {
            this.prepareCacheSize = prepareCacheSize;
            return this;
        }

        /**
         * Configures the compression algorithms.  Default to [{@link CompressionAlgorithm#UNCOMPRESSED}].
         * <p>
         * It will auto choose an algorithm that's contained in the list and supported by the server,
         * preferring zstd, then zlib. If the list does not contain {@link CompressionAlgorithm#UNCOMPRESSED}
         * and the server does not support any algorithm in the list, an exception will be thrown when
         * connecting.
         * <p>
         * Note: zstd requires a dependency {@code com.github.luben:zstd-jni}.
         *
         * @param compressionAlgorithms the list of compression algorithms.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code compressionAlgorithms} is {@code null} or empty.
         * @since 1.1.2
         */
        public Builder compressionAlgorithms(CompressionAlgorithm... compressionAlgorithms) {
            requireNonNull(compressionAlgorithms, "compressionAlgorithms must not be null");
            require(compressionAlgorithms.length != 0, "compressionAlgorithms must not be empty");

            if (compressionAlgorithms.length == 1) {
                requireNonNull(compressionAlgorithms[0], "compressionAlgorithms must not contain null");
                this.compressionAlgorithms = Collections.singleton(compressionAlgorithms[0]);
            } else {
                Set<CompressionAlgorithm> algorithms = EnumSet.noneOf(CompressionAlgorithm.class);

                for (CompressionAlgorithm algorithm : compressionAlgorithms) {
                    requireNonNull(algorithm, "compressionAlgorithms must not contain null");
                    algorithms.add(algorithm);
                }

                this.compressionAlgorithms = algorithms;
            }

            return this;
        }

        /**
         * Configures the zstd compression level.  Default to {@code 3}.
         * <p>
         * It is only used if zstd is chosen for the connection.
         * <p>
         * Note: MySQL protocol does not allow to set the zlib compression level of the server, only zstd is
         * configurable.
         *
         * @param level the compression level.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code level} is not between 1 and 22.
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/connection-options.html">
         * MySQL Connection Options --zstd-compression-level</a>
         * @since 1.1.2
         */
        public Builder zstdCompressionLevel(int level) {
            require(level >= 1 && level <= 22, "level must be between 1 and 22");

            this.zstdCompressionLevel = level;
            return this;
        }

        /**
         * Configures the {@link LoopResources} for the driver. Default to
         * {@link TcpResources#get() global tcp resources}.
         *
         * @param loopResources the {@link LoopResources}.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code loopResources} is {@code null}.
         * @since 1.1.2
         */
        public Builder loopResources(LoopResources loopResources) {
            this.loopResources = requireNonNull(loopResources, "loopResources must not be null");
            return this;
        }

        /**
         * Configures whether to use {@link ServiceLoader} to discover and register extensions. Defaults to
         * {@code true}.
         *
         * @param enabled to discover and register extensions.
         * @return this {@link Builder}.
         * @since 0.8.2
         */
        public Builder autodetectExtensions(boolean enabled) {
            this.autodetectExtensions = enabled;
            return this;
        }

        /**
         * Registers a {@link Extension} to extend driver functionality and manually.
         * <p>
         * Notice: the driver will not deduplicate {@link Extension}s of autodetect discovered and manually
         * extended. So if a {@link Extension} is registered by this function and autodetect discovered, it
         * will get two {@link Extension} as same.
         *
         * @param extension extension to extend driver functionality.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code extension} is {@code null}.
         * @since 0.8.2
         */
        public Builder extendWith(Extension extension) {
            this.extensions.add(requireNonNull(extension, "extension must not be null"));
            return this;
        }

        /**
         * Registers a password publisher function.
         *
         * @param passwordPublisher function to retrieve password before making connection.
         * @return this {@link Builder}.
         */
        public Builder passwordPublisher(Publisher<String> passwordPublisher) {
            this.passwordPublisher = passwordPublisher;
            return this;
        }

        /**
         * Sets the {@link AddressResolverGroup} for resolving host addresses.
         * <p>
         * This can be used to customize the DNS resolution mechanism, which is particularly useful in environments
         * with specific DNS configuration needs or where a custom DNS resolver is required.
         *
         * @param resolver the resolver group to use for host address resolution.
         * @return this {@link Builder}.
         * @since 1.2.0
         */
        public Builder resolver(AddressResolverGroup<?> resolver) {
            this.resolver = resolver;
            return this;
        }

        /**
         * Option to enable metrics to be collected and registered in Micrometer's globalRegistry
         * with {@link reactor.netty.tcp.TcpClient#metrics(boolean)}. Defaults to {@code false}.
         * <p>
         * Note: It is required to add {@code io.micrometer.micrometer-core} dependency to classpath.
         *
         * @param enabled enable metrics for {@link reactor.netty.tcp.TcpClient}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code io.micrometer:micrometer-core} is not on the classpath.
         * @since 1.3.2
         */
        public Builder metrics(boolean enabled) {
            require(!enabled || Metrics.isMicrometerAvailable(),
            "dependency `io.micrometer:micrometer-core` must be added to classpath if metrics enabled"
            );
            this.metrics = enabled;
            return this;
        }

        /**
         * Option to whether the driver should interpret MySQL's TINYINT(1) as a BIT type.
         * When enabled, TINYINT(1) columns (both SIGNED and UNSIGNED) will be treated as
         * BIT. default to {@code true}.
         *
         * @param tinyInt1isBit {@code true} to treat TINYINT(1) as BIT
         * @return this {@link Builder}
         * @since 1.4.0
         */
        public Builder tinyInt1isBit(boolean tinyInt1isBit) {
            this.tinyInt1isBit = tinyInt1isBit;
            return this;
        }

        private SslMode requireSslMode() {
            SslMode sslMode = this.sslMode;

            if (sslMode == null) {
                sslMode = isHost ? SslMode.PREFERRED : SslMode.DISABLED;
            }

            return sslMode;
        }

        private Builder() { }
    }
}
