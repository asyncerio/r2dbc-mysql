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

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.api.MySqlColumnMetadata;
import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.api.MySqlRow;
import io.asyncer.r2dbc.mysql.api.MySqlRowMetadata;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Base class considers integration tests for MariaDB.
 */
abstract class MariaDbIntegrationTestSupport extends IntegrationTestSupport {

    MariaDbIntegrationTestSupport(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {
        super(configuration(customizer));
    }

    @Test
    void returningExpression() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,value INT NOT NULL)")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?)")
                .bind(0, 2)
                .returnGeneratedValues("POW(value, 4)")
                .execute())
            .flatMap(result -> result.map(r -> r.get(0, Integer.class)))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(1))
            .doOnNext(list -> assertThat(list.get(0)).isEqualTo(16)));
    }

    @Test
    void allReturning() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                "value INT NOT NULL," +
                "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?),(?),(?),(?),(?)")
                .bind(0, 2)
                .bind(1, 4)
                .bind(2, 6)
                .bind(3, 8)
                .bind(4, 10)
                .returnGeneratedValues()
                .execute())
            .flatMap(result -> result.map(DataEntity::read))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5))
            .as(list -> assertWithSelectAll(conn, list))
            .thenMany(conn.createStatement("REPLACE test(id,value) VALUES (1,?),(2,?),(3,?),(4,?),(5,?)")
                .bind(0, 3)
                .bind(1, 5)
                .bind(2, 7)
                .bind(3, 9)
                .bind(4, 11)
                .returnGeneratedValues()
                .execute())
            .flatMap(result -> result.map(DataEntity::read))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5))
            .as(list -> assertWithSelectAll(conn, list)));
    }

    @Test
    void partialReturning() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                "value INT NOT NULL," +
                "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?),(?),(?),(?),(?)")
                .bind(0, 2)
                .bind(1, 4)
                .bind(2, 6)
                .bind(3, 8)
                .bind(4, 10)
                .returnGeneratedValues("id", "value")
                .execute())
            .flatMap(result -> result.map(DataEntity::withoutCreatedAt))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5))
            .as(list -> assertWithoutCreatedAt(conn, list))
            .thenMany(conn.createStatement("REPLACE test(id, value) VALUES (1,?),(2,?),(3,?),(4,?),(5,?)")
                .bind(0, 3)
                .bind(1, 5)
                .bind(2, 7)
                .bind(3, 9)
                .bind(4, 11)
                .returnGeneratedValues("id", "value")
                .execute())
            .flatMap(result -> result.map(DataEntity::withoutCreatedAt))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5))
            .as(list -> assertWithoutCreatedAt(conn, list)));
    }

    @Test
    void returningGetRowUpdated() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test(" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,value INT NOT NULL)")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?),(?)")
                .bind(0, 2)
                .bind(1, 4)
                .returnGeneratedValues()
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(it -> assertThat(it).isEqualTo(2)));
    }

    @Test
    @EnabledIf("envIsMariaDb10_5_1")
    void returningExtendedTypeInfoJson() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test(" +
            "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, value JSON NOT NULL)")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?)")
                .bind(0, "{\"abc\": 123}")
                .returnGeneratedValues()
                .execute())
            .flatMap(result -> result.map(DataEntity::readExtendedTypeInfoResult))
            .collectList()
            .doOnNext(list -> assertIfExtendedTypeInfoEnabled(conn, list))
            );
    }

    private static Mono<Void> assertWithSelectAll(MySqlConnection conn, Mono<List<DataEntity>> returning) {
        return returning.zipWhen(list -> conn.createStatement("SELECT * FROM test WHERE id IN (?,?,?,?,?)")
                .bind(0, list.get(0).getId())
                .bind(1, list.get(1).getId())
                .bind(2, list.get(2).getId())
                .bind(3, list.get(3).getId())
                .bind(4, list.get(4).getId())
                .execute()
                .flatMap(result -> result.map(DataEntity::read))
                .collectList())
            .doOnNext(list -> assertThat(list.getT1()).isEqualTo(list.getT2()))
            .then();
    }

    private static Mono<Void> assertWithoutCreatedAt(MySqlConnection conn, Mono<List<DataEntity>> returning) {
        String sql = "SELECT id,value FROM test WHERE id IN (?,?,?,?,?)";

        return returning.zipWhen(list -> conn.createStatement(sql)
                .bind(0, list.get(0).getId())
                .bind(1, list.get(1).getId())
                .bind(2, list.get(2).getId())
                .bind(3, list.get(3).getId())
                .bind(4, list.get(4).getId())
                .execute()
                .flatMap(result -> result.map(DataEntity::withoutCreatedAt))
                .collectList())
            .doOnNext(list -> assertThat(list.getT1()).isEqualTo(list.getT2()))
            .then();
    }

    private static void assertIfExtendedTypeInfoEnabled(MySqlConnection conn, List<Boolean> list) {
        boolean enabled = ((MySqlSimpleConnection)conn).context().getCapability().isExtendedTypeInfo();
        if (enabled) {
            assertThat(list.get(0)).isEqualTo(true);
        } else {
            assertThat(list.get(0)).isEqualTo(false);
        }
    }

    private static final class DataEntity {

        private final int id;

        private final int value;

        private final ZonedDateTime createdAt;

        private DataEntity(int id, int value, ZonedDateTime createdAt) {
            this.id = id;
            this.value = value;
            this.createdAt = createdAt;
        }

        int getId() {
            return id;
        }

        int getValue() {
            return value;
        }

        ZonedDateTime getCreatedAt() {
            return createdAt;
        }

        DataEntity incremented() {
            return new DataEntity(id, value + 1, createdAt);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DataEntity)) {
                return false;
            }

            DataEntity that = (DataEntity) o;

            return id == that.id && value == that.value && createdAt.equals(that.createdAt);
        }

        @Override
        public int hashCode() {
            int result = 31 * id + value;
            return 31 * result + createdAt.hashCode();
        }

        @Override
        public String toString() {
            return "DataEntity{id=" + id +
                ", value=" + value +
                ", createdAt=" + createdAt +
                '}';
        }

        static DataEntity read(Readable readable) {
            Integer id = readable.get("id", Integer.TYPE);
            Integer value = readable.get("value", Integer.class);
            ZonedDateTime createdAt = readable.get("created_at", ZonedDateTime.class);

            requireNonNull(id, "id must not be null");
            requireNonNull(value, "value must not be null");
            requireNonNull(createdAt, "createdAt must not be null");

            return new DataEntity(id, value, createdAt);
        }

        static DataEntity withoutCreatedAt(Readable readable) {
            Integer id = readable.get("id", Integer.TYPE);
            Integer value = readable.get("value", Integer.TYPE);

            requireNonNull(id, "id must not be null");
            requireNonNull(value, "value must not be null");

            return new DataEntity(id, value, ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC));
        }

        static Boolean readExtendedTypeInfoResult(Row row, RowMetadata rowMetadata) {
            Boolean extendedTypeInfoResult = ((MySqlRowMetadata)rowMetadata)
                .getColumnMetadata("value").getNativeTypeMetadata().isMariaDbJson();
            return extendedTypeInfoResult;
        }
    }
}
