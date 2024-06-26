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

package tech.ydb.io.r2dbc.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Egor Kuleshov
 */
public class YdbLookupTest {
    @Test
    public void resolveFilePath() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("file:/root.file");
        assertEquals(Optional.of(new URL("file:/root.file")), url);
    }

    @Test
    public void resolveFilePathFromHome() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("file:~/home.file");
        String home = System.getProperty("user.home");
        assertEquals(Optional.of(new URL("file:" + home + "/home.file")), url);
    }

    @Test
    public void resolveFilePathFromHomePure() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("~/home.file");
        String home = System.getProperty("user.home");
        assertEquals(Optional.of(new URL("file:" + home + "/home.file")), url);
    }
}
