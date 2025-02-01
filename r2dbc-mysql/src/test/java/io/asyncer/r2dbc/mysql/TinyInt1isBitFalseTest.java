/*
 * Copyright 2025 asyncer.io projects
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


import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

class TinyInt1isBitFalseTest extends IntegrationTestSupport{
    TinyInt1isBitFalseTest() {
        super(configuration(builder -> builder.tinyInt1isBit(false)));
    }

    @Test
    public void tinyInt1isBitFalse() {
        complete(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE `test` (`id` INT NOT NULL PRIMARY KEY, `value` TINYINT(1))").execute())
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .thenMany(connection.createStatement("INSERT INTO `test` VALUES (1, 1)").execute())
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .thenMany(connection.createStatement("SELECT `value` FROM `test`").execute())
                                   .flatMap(result -> result.map((row, metadata) -> row.get("value", Object.class)))
                                   .doOnNext(value -> assertThat(value).isInstanceOf(Byte.class)));
    }

}
