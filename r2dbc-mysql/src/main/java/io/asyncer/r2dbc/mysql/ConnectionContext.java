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

import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.r2dbc.spi.IsolationLevel;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * The MySQL connection context considers the behavior of server or client.
 * <p>
 * WARNING: Do NOT change any data outside of this project, try to configure {@code ConnectionFactoryOptions} or
 * {@code MySqlConnectionConfiguration} to control connection context and client behavior.
 */
public final class ConnectionContext implements CodecContext {

    private static final ServerVersion NONE_VERSION = ServerVersion.create(0, 0, 0);

    private static final ServerVersion MYSQL_5_7_4 = ServerVersion.create(5, 7, 4);

    private static final ServerVersion MARIA_10_1_1 = ServerVersion.create(10, 1, 1, true);

    private final ZeroDateOption zeroDateOption;

    @Nullable
    private final Path localInfilePath;

    private final int localInfileBufferSize;

    private final boolean tinyInt1isBit;

    private final boolean preserveInstants;

    private int connectionId = -1;

    private ServerVersion serverVersion = NONE_VERSION;

    private Capability capability = Capability.DEFAULT;

    private PrepareCache prepareCache;

    @Nullable
    private ZoneId timeZone;

    private String product = "Unknown";

    /**
     * Current isolation level inferred by past statements.
     * <p>
     * Inference rules:
     * <ol><li>In the beginning, it is also {@link #sessionIsolationLevel}.</li>
     * <li>A transaction has began with a {@link IsolationLevel}, it will be changed to the value</li>
     * <li>The transaction end (commit or rollback), it will recover to {@link #sessionIsolationLevel}.</li></ol>
     */
    private volatile IsolationLevel currentIsolationLevel;

    /**
     * Session isolation level.
     *
     * <ol><li>It is applied to all subsequent transactions performed within the current session.</li>
     * <li>Calls {@link io.r2dbc.spi.Connection#setTransactionIsolationLevel}, it will change to the value.</li>
     * <li>It can be changed within transactions, but does not affect the current ongoing transaction.</li></ol>
     */
    private volatile IsolationLevel sessionIsolationLevel;

    private boolean lockWaitTimeoutSupported = false;

    /**
     * Current lock wait timeout in seconds.
     */
    private volatile Duration currentLockWaitTimeout;

    /**
     * Session lock wait timeout in seconds.
     */
    private volatile Duration sessionLockWaitTimeout;

    /**
     * Assume that the auto commit is always turned on, it will be set after handshake V10 request message, or OK
     * message which means handshake V9 completed.
     */
    private volatile short serverStatuses = ServerStatuses.AUTO_COMMIT;

    ConnectionContext(
        ZeroDateOption zeroDateOption,
        @Nullable Path localInfilePath,
        int localInfileBufferSize,
        boolean tinyInt1isBit,
        boolean preserveInstants,
        @Nullable ZoneId timeZone
    ) {
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.localInfilePath = localInfilePath;
        this.localInfileBufferSize = localInfileBufferSize;
        this.tinyInt1isBit = tinyInt1isBit;
        this.preserveInstants = preserveInstants;
        this.timeZone = timeZone;
    }

    /**
     * Initializes handshake information after connection is established.
     *
     * @param connectionId the connection identifier that is specified by server.
     * @param version      the server version.
     * @param capability   the connection capabilities.
     */
    void initHandshake(int connectionId, ServerVersion version, Capability capability) {
        this.connectionId = connectionId;
        this.serverVersion = version;
        this.capability = capability;
    }

    /**
     * Initializes session information after logged-in.
     *
     * @param prepareCache             the prepare cache.
     * @param isolationLevel           the session isolation level.
     * @param lockWaitTimeoutSupported if the server supports lock wait timeout.
     * @param lockWaitTimeout          the lock wait timeout.
     * @param product                  the server product name.
     * @param timeZone                 the server timezone.
     */
    void initSession(
        PrepareCache prepareCache,
        IsolationLevel isolationLevel,
        boolean lockWaitTimeoutSupported,
        Duration lockWaitTimeout,
        @Nullable String product,
        @Nullable ZoneId timeZone
    ) {
        this.prepareCache = prepareCache;
        this.currentIsolationLevel = this.sessionIsolationLevel = isolationLevel;
        this.lockWaitTimeoutSupported = lockWaitTimeoutSupported;
        this.currentLockWaitTimeout = this.sessionLockWaitTimeout = lockWaitTimeout;
        this.product = product == null ? "Unknown" : product;

        if (timeZone != null) {
            if (isTimeZoneInitialized()) {
                throw new IllegalStateException("Connection timezone have been initialized");
            }
            this.timeZone = timeZone;
        }
    }

    /**
     * Get the connection identifier that is specified by server.
     *
     * @return the connection identifier.
     */
    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    public Capability getCapability() {
        return capability;
    }

    @Override
    public CharCollation getClientCollation() {
        return CharCollation.clientCharCollation();
    }

    @Override
    public boolean isPreserveInstants() {
        return preserveInstants;
    }

    @Override
    public ZoneId getTimeZone() {
        if (timeZone == null) {
            throw new IllegalStateException("Server timezone have not initialization");
        }
        return timeZone;
    }

    String getProduct() {
        return product;
    }

    PrepareCache getPrepareCache() {
        return prepareCache;
    }

    boolean isTimeZoneInitialized() {
        return timeZone != null;
    }

    @Override
    public boolean isMariaDb() {
        Capability capability = this.capability;
        return (capability != null && capability.isMariaDb()) || serverVersion.isMariaDb();
    }

    @Override
    public boolean isTinyInt1isBit() {
        return tinyInt1isBit;
    }

    public boolean isNoBackslashEscapes() {
        return (serverStatuses & ServerStatuses.NO_BACKSLASH_ESCAPES) != 0;
    }

    @Override
    public ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    /**
     * Gets the allowed local infile path.
     *
     * @return the path.
     */
    @Nullable
    public Path getLocalInfilePath() {
        return localInfilePath;
    }

    /**
     * Gets the local infile buffer size.
     *
     * @return the buffer size.
     */
    public int getLocalInfileBufferSize() {
        return localInfileBufferSize;
    }

    /**
     * Checks if the server supports InnoDB lock wait timeout.
     *
     * @return if the server supports InnoDB lock wait timeout.
     */
    public boolean isLockWaitTimeoutSupported() {
        return lockWaitTimeoutSupported;
    }

    /**
     * Checks if the server supports statement timeout.
     *
     * @return if the server supports statement timeout.
     */
    public boolean isStatementTimeoutSupported() {
        boolean isMariaDb = isMariaDb();
        return (isMariaDb && serverVersion.isGreaterThanOrEqualTo(MARIA_10_1_1)) ||
            (!isMariaDb && serverVersion.isGreaterThanOrEqualTo(MYSQL_5_7_4));
    }

    /**
     * Get the bitmap of server statuses.
     *
     * @return the bitmap.
     */
    public short getServerStatuses() {
        return serverStatuses;
    }

    /**
     * Updates server statuses.
     *
     * @param serverStatuses the bitmap of server statuses.
     */
    public void setServerStatuses(short serverStatuses) {
        this.serverStatuses = serverStatuses;
    }

    IsolationLevel getCurrentIsolationLevel() {
        return currentIsolationLevel;
    }

    void setCurrentIsolationLevel(IsolationLevel isolationLevel) {
        this.currentIsolationLevel = isolationLevel;
    }

    void resetCurrentIsolationLevel() {
        this.currentIsolationLevel = this.sessionIsolationLevel;
    }

    IsolationLevel getSessionIsolationLevel() {
        return sessionIsolationLevel;
    }

    void setSessionIsolationLevel(IsolationLevel isolationLevel) {
        this.sessionIsolationLevel = isolationLevel;
    }

    void setCurrentLockWaitTimeout(Duration timeoutSeconds) {
        this.currentLockWaitTimeout = timeoutSeconds;
    }

    void resetCurrentLockWaitTimeout() {
        this.currentLockWaitTimeout = this.sessionLockWaitTimeout;
    }

    boolean isLockWaitTimeoutChanged() {
        return currentLockWaitTimeout != sessionLockWaitTimeout;
    }

    Duration getSessionLockWaitTimeout() {
        return sessionLockWaitTimeout;
    }

    void setAllLockWaitTimeout(Duration timeoutSeconds) {
        this.currentLockWaitTimeout = this.sessionLockWaitTimeout = timeoutSeconds;
    }

    boolean isInTransaction() {
        return (serverStatuses & ServerStatuses.IN_TRANSACTION) != 0;
    }

    boolean isAutoCommit() {
        // Within transaction, autocommit remains disabled until end the transaction with COMMIT or ROLLBACK.
        // The autocommit mode then reverts to its previous state.
        short serverStatuses = this.serverStatuses;
        return (serverStatuses & ServerStatuses.IN_TRANSACTION) == 0 &&
            (serverStatuses & ServerStatuses.AUTO_COMMIT) != 0;
    }
}
