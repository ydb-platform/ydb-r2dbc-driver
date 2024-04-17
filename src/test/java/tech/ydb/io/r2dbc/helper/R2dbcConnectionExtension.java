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

package tech.ydb.io.r2dbc.helper;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.junit.Assert;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import tech.ydb.io.r2dbc.YdbConnection;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.table.TableClient;
import tech.ydb.test.integration.YdbHelperFactory;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * @author Egor Kuleshov
 */
public class R2dbcConnectionExtension implements ExecutionCondition,
        BeforeEachCallback, BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final Map<ExtensionContext, YdbConnection> map = new HashMap<>();
    private final Stack<YdbConnection> stack = new Stack<>();
    private final YdbContext ydbContext;

    public R2dbcConnectionExtension(YdbHelperExtension ydb) {
        ydbContext = new YdbContext(TableClient.newClient(ydb.createTransport()).build());
    }

    private void register(ExtensionContext ctx) {
        Assert.assertFalse("Dublicate of context registration", map.containsKey(ctx));

        YdbConnection connection = new YdbConnection(ydbContext);
        map.put(ctx, connection);
        stack.push(connection);
    }

    private void unregister(ExtensionContext ctx) {
        Assert.assertFalse("Extra unregister call", stack.isEmpty());
        Assert.assertEquals("Top connection must be unregistered first", stack.peek(), map.get(ctx));

        stack.pop().close().block();
        map.remove(ctx);
    }

    public YdbConnection connection() {
        Assert.assertFalse("Retrive connection before initialization", stack.isEmpty());
        return stack.peek();
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (!YdbHelperFactory.getInstance().isEnabled()) {
            return ConditionEvaluationResult.disabled("Ydb helper is disabled " + context.getDisplayName());
        }

        return ConditionEvaluationResult.enabled("OK");
    }

    @Override
    public void beforeEach(ExtensionContext ctx) {
        register(ctx);
    }

    @Override
    public void afterEach(ExtensionContext ctx) {
        unregister(ctx);
    }

    @Override
    public void beforeAll(ExtensionContext ctx) {
        register(ctx);
    }

    @Override
    public void afterAll(ExtensionContext ctx) {
        unregister(ctx);
    }
}
