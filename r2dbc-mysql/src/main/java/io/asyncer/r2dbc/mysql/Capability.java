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

/**
 * A capabilities flag bitmap considers to define the session behaviors of the connection.
 * <p>
 * Some flags should not be disabled by driver or always enabled. These flags may have no getter or builder
 * setter, or neither.
 */
public final class Capability {

    /**
     * If UNSET, the server supports the MariaDB protocol and statements.
     */
    private static final long CLIENT_MYSQL = 1L;

    /**
     * Use found/touched rows instead of changed rows for affected rows. Should enable it by default.
     */
    private static final long FOUND_ROWS = 2L;

    /**
     * Use 2-bytes definition flags of {@code DefinitionMetadataMessage}.
     * <p>
     * Very old servers (before 3.23) will not set this capability flag.
     */
    private static final long LONG_FLAG = 4L;

    /**
     * Connect to server with a database.
     */
    private static final long CONNECT_WITH_DB = 8L;

    /**
     * Enable it to disallow a statement which use {@code database.table.column} for access other schema
     * data.
     */
    private static final long NO_SCHEMA = 16L;

    /**
     * The deflate compression, old compression flag.
     */
    private static final long COMPRESS = 32L;

//    private static final long ODBC = 64L; // R2DBC driver is not ODBC driver.

    /**
     * Allow to use LOAD DATA [LOCAL] INFILE statement.
     */
    private static final long LOCAL_FILES = 128L;

    /**
     * Ignore space between a built-in function name and the subsequent parenthesis.
     * <p>
     * Note: Ignoring spaces may cause ambiguity.
     * <p>
     * See also <a href="https://dev.mysql.com/doc/refman/8.0/en/function-resolution.html">
     * Function Resolution</a>.
     */
    private static final long IGNORE_SPACE = 256L;

    /**
     * The protocol version is 4.1 (instead of 3.20).
     */
    private static final long PROTOCOL_41 = 512L;

    /**
     * Use {@code wait_interactive_timeout} instead of {@code wait_timeout} for the server waits for activity
     * on a connection before closing it.
     */
    private static final long INTERACTIVE = 1024L;

    /**
     * Enable SSL.
     */
    private static final long SSL = 2048L;

//    private static final long IGNORE_SIGPIPE = 4096L; // Connector/C only flag.

    /**
     * Allow transactions. All available versions of MySQL server support it.
     */
    private static final long TRANSACTIONS = 8192L;

    // Old flag and alias of PROTOCOL_41. It will not be used by any available server version/edition.
//    private static final int RESERVED = 16384L;

    /**
     * Allow second part of authentication hashing salt.
     * <p>
     * Old name: RESERVED2.
     * <p>
     * Origin name: SECURE_CONNECTION.
     */
    private static final long SECURE_SALT = 32768L;

    /**
     * Allow to send multiple statements in text query and prepare query.
     * <p>
     * Old name: MULTI_QUERIES.
     */
    private static final long MULTI_STATEMENTS = 65536L;

    /**
     * Allow to receive multiple results in the response of executing a text query.
     */
    private static final long MULTI_RESULTS = 1L << 17;

    /**
     * Allow to receive multiple results in the response of executing a prepare query.
     */
    private static final long PS_MULTI_RESULTS = 1L << 18;

    /**
     * Supports authentication plugins. Server will send more details (i.e. name) for authentication plugin.
     */
    private static final long PLUGIN_AUTH = 1L << 19;

    /**
     * Connection attributes should be sent.
     */
    private static final long CONNECT_ATTRS = 1L << 20;

    /**
     * Can use var-integer sized bytes to encode client authentication.
     * <p>
     * Origin name: PLUGIN_AUTH_LENENC_CLIENT_DATA.
     */
    private static final long VAR_INT_SIZED_AUTH = 1L << 21;

//    private static final long HANDLE_EXPIRED_PASSWORD = 1L << 22; // Client can handle expired passwords.

    /**
     * Server can send session state information in the OK packet.
     */
    private static final long SESSION_TRACK = 1L << 23;

    /**
     * The MySQL server marks the EOF message as deprecated and use OK message instead.
     */
    private static final long DEPRECATE_EOF = 1L << 24;

    // Allow the server not to send column metadata in result set,
    // should NEVER enable this option.
//    private static final long OPTIONAL_RESULT_SET_METADATA = 1L << 25;
    private static final long ZSTD_COMPRESS = 1L << 26;

    // A reserved flag, used to extend the 32-bits capability bitmap to 64-bits.
    // There is no available MySql server version/edition to support it.
//    private static final long CAPABILITY_EXTENSION = 1L << 29;
//    private static final long SSL_VERIFY_SERVER_CERT = 1L << 30; // Client only flag, use SslMode instead.
//    private static final long REMEMBER_OPTIONS = 1L << 31; // Connector/C only flag.

//    private static final long MARIADB_CLIENT_PROGRESS = 1L << 32;
//    private static final long MARIADB_CLIENT_COM_MULTI = 1L << 33;
//    private static final long MARIADB_CLIENT_STMT_BULK_OPERATIONS = 1L << 34;

    /**
     * Receive extended column type information from MariaDB to find out more specific details about column type.
     */
    private static final long MARIADB_CLIENT_EXTENDED_TYPE_INFO = 1L << 35;
//    private static final long MARIADB_CLIENT_CACHE_METADATA = 1L << 36;

    private static final long ALL_SUPPORTED = CLIENT_MYSQL | FOUND_ROWS | LONG_FLAG | CONNECT_WITH_DB |
        NO_SCHEMA | COMPRESS | LOCAL_FILES | IGNORE_SPACE | PROTOCOL_41 | INTERACTIVE | SSL |
        TRANSACTIONS | SECURE_SALT | MULTI_STATEMENTS | MULTI_RESULTS | PS_MULTI_RESULTS |
        PLUGIN_AUTH | CONNECT_ATTRS | VAR_INT_SIZED_AUTH | SESSION_TRACK | DEPRECATE_EOF | ZSTD_COMPRESS |
        MARIADB_CLIENT_EXTENDED_TYPE_INFO;

    /**
     * The default capabilities for a MySQL connection. It contains all client supported capabilities.
     */
    public static final Capability DEFAULT = new Capability(ALL_SUPPORTED);

    private final long bitmap;

    /**
     * Checks if the connection is using MariaDB capabilities.
     *
     * @return if using MariaDB capabilities.
     */
    public boolean isMariaDb() {
        return (bitmap & CLIENT_MYSQL) == 0;
    }

    /**
     * Checks if the connection will be connected and logon with a database.
     *
     * @return if login with database.
     */
    public boolean isConnectWithDatabase() {
        return (bitmap & CONNECT_WITH_DB) != 0;
    }

    /**
     * Checks if the connection enabled SSL.
     *
     * @return if SSL enabled.
     */
    public boolean isSslEnabled() {
        return (bitmap & SSL) != 0;
    }

    /**
     * Checks if the connection is using protocol 4.1.
     *
     * @return if using protocol 4.1.
     */
    public boolean isProtocol41() {
        return (bitmap & PROTOCOL_41) != 0;
    }

    /**
     * Checks if allow to use var-integer sized bytes to encode client authentication.
     *
     * @return if allow to use var-integer sized authentication.
     */
    public boolean isVarIntSizedAuthAllowed() {
        return (bitmap & VAR_INT_SIZED_AUTH) != 0;
    }

    /**
     * Checks if allow authentication plugin type name.
     *
     * @return if allowed.
     */
    public boolean isPluginAuthAllowed() {
        return (bitmap & PLUGIN_AUTH) != 0;
    }

    /**
     * Checks if the connection contains connection attributes.
     *
     * @return if connection attributes exists.
     */
    public boolean isConnectionAttributesAllowed() {
        return (bitmap & CONNECT_ATTRS) != 0;
    }

    /**
     * Checks if server supports multiple-statement. i.e. computed statement.
     *
     * @return if server supported.
     */
    public boolean isMultiStatementsAllowed() {
        return (bitmap & MULTI_STATEMENTS) != 0;
    }

    /**
     * Checks if server marks EOF message as deprecated.
     *
     * @return if EOF message was deprecated.
     */
    public boolean isEofDeprecated() {
        return (bitmap & DEPRECATE_EOF) != 0;
    }

    /**
     * Checks if server uses more than 8 bytes of salt.
     *
     * @return if using secure salt.
     */
    public boolean isSaltSecured() {
        return (bitmap & SECURE_SALT) != 0;
    }

    /**
     * Checks if server supports transaction.
     *
     * @return if server supported.
     */
    public boolean isTransactionAllowed() {
        return (bitmap & TRANSACTIONS) != 0;
    }

    /**
     * Checks if any compression enabled.
     *
     * @return if any compression enabled.
     */
    public boolean isCompression() {
        return (bitmap & (COMPRESS | ZSTD_COMPRESS)) != 0;
    }

    /**
     * Checks if zlib compression enabled.
     *
     * @return if zlib compression enabled.
     */
    public boolean isZlibCompression() {
        return (bitmap & COMPRESS) != 0;
    }

    /**
     * Checks if zstd compression enabled.
     *
     * @return if zstd compression enabled.
     */
    public boolean isZstdCompression() {
        return (bitmap & ZSTD_COMPRESS) != 0;
    }

    /**
     * Checks if MariaDB extended type info enabled.
     *
     * @return if MariaDB extended type info enabled.
     */
    public boolean isExtendedTypeInfo() {
        return (bitmap & MARIADB_CLIENT_EXTENDED_TYPE_INFO) != 0;
    }

    /**
     * Extends MariaDB capabilities.
     *
     * @param hiCapabilities the bitmap of extend capabilities.
     * @return a new {@link Capability} takes base and extend capabilities.
     */
    public Capability extendMariaDb(long hiCapabilities) {
        return of((this.bitmap & 0xFFFFFFFFL) | (hiCapabilities << 32));
    }

    /**
     * Get the lower 32-bits bitmap of {@link Capability this}.
     *
     * @return the lower 32-bits bitmap.
     */
    public int getBaseBitmap() {
        return (int) bitmap;
    }

    /**
     * Get the higher 32-bits bitmap of {@link Capability this}.
     *
     * @return the higher 32-bits bitmap.
     */
    public int getExtendBitmap() {
        return (int) (bitmap >>> 32);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Capability)) {
            return false;
        }

        Capability that = (Capability) o;

        return bitmap == that.bitmap;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(bitmap);
    }

    @Override
    public String toString() {
        // Do not consider complex output, just use hex.
        return "Capability<0x" + Long.toHexString(bitmap) + '>';
    }

    Builder mutate() {
        return new Builder(bitmap);
    }

    private Capability(long bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * Creates a {@link Capability} with capabilities bitmap. It will unset all unknown flags.
     *
     * @param capabilities the bitmap of capabilities.
     * @return the {@link Capability} without unknown flags.
     */
    public static Capability of(long capabilities) {
        long c = capabilities & ALL_SUPPORTED;
        return c == ALL_SUPPORTED ? DEFAULT : new Capability(c);
    }

    static final class Builder {

        private long bitmap;

        void disableConnectWithDatabase() {
            this.bitmap &= ~CONNECT_WITH_DB;
        }

        void disableDatabasePinned() {
            this.bitmap &= ~NO_SCHEMA;
        }

        void disableCompression() {
            this.bitmap &= ~(COMPRESS | ZSTD_COMPRESS);
        }

        void disableZlibCompression() {
            this.bitmap &= ~COMPRESS;
        }

        void disableZstdCompression() {
            this.bitmap &= ~ZSTD_COMPRESS;
        }

        void disableLoadDataLocalInfile() {
            this.bitmap &= ~LOCAL_FILES;
        }

        void disableIgnoreAmbiguitySpace() {
            this.bitmap &= ~IGNORE_SPACE;
        }

        void disableInteractiveTimeout() {
            this.bitmap &= ~INTERACTIVE;
        }

        void disableSsl() {
            this.bitmap &= ~SSL;
        }

        void disableSessionTrack() {
            this.bitmap &= ~SESSION_TRACK;
        }

        void disableConnectAttributes() {
            this.bitmap &= ~CONNECT_ATTRS;
        }

        Capability build() {
            return of(this.bitmap);
        }

        private Builder(long bitmap) {
            this.bitmap = bitmap;
        }
    }
}
