/*
 * Copyright 2022 YANDEX LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.ydb.io.r2dbc;

import com.google.common.base.Preconditions;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

/**
 * @author Kirill Kurdyukov
 */
public final class YdbConnectionFactoryProvider implements ConnectionFactoryProvider {

    private static final String YDB_DRIVER = "ydb";

    @Override
    public YdbConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        return new YdbConnectionFactory(new YdbContext(new OptionExtractor(connectionFactoryOptions)));
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Preconditions.checkNotNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        Object driver = connectionFactoryOptions.getValue(ConnectionFactoryOptions.DRIVER);

        return driver != null && driver.equals(YDB_DRIVER);
    }

    @Override
    public String getDriver() {
        return YDB_DRIVER;
    }
}
