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

import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.MySqlType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MySqlTypeMetadata}.
 */
class MySqlTypeMetadataTest {

    @Test
    void allSet() {
        MySqlTypeMetadata metadata = new MySqlTypeMetadata(0, -1, 0, null);

        assertThat(metadata.isBinary()).isTrue();
        assertThat(metadata.isSet()).isTrue();
        assertThat(metadata.isUnsigned()).isTrue();
        assertThat(metadata.isEnum()).isTrue();
        assertThat(metadata.isNotNull()).isTrue();
    }

    @Test
    void noSet() {
        MySqlTypeMetadata metadata = new MySqlTypeMetadata(0, 0, 0, null);

        assertThat(metadata.isBinary()).isFalse();
        assertThat(metadata.isSet()).isFalse();
        assertThat(metadata.isUnsigned()).isFalse();
        assertThat(metadata.isEnum()).isFalse();
        assertThat(metadata.isNotNull()).isFalse();
    }

    @Test
    void isBinaryUsesCollationId() {
        MySqlTypeMetadata metadata = new MySqlTypeMetadata(0, -1, CharCollation.BINARY_ID, null);

        assertThat(metadata.isBinary()).isTrue();

        metadata = new MySqlTypeMetadata(0, -1, 33, null);
        assertThat(metadata.isBinary()).isFalse();
    }
    
    @Test
    void mariaDbJsonReturnsCorrectMySqlType() {
    	MySqlTypeMetadata metadata = new MySqlTypeMetadata(254, 0, 0, "json");
    	
    	assertThat(metadata.isMariaDbJson()).isTrue();
    	assertThat(MySqlType.of(metadata)).isEqualTo(MySqlType.JSON);
    	
    	metadata = new MySqlTypeMetadata(254, 0 ,0 , null);
    	
    	assertThat(metadata.isMariaDbJson()).isFalse();
    	assertThat(MySqlType.of(metadata)).isEqualTo(MySqlType.VARCHAR);
    }
}
