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

package tech.ydb.io.r2dbc.options;

import io.r2dbc.spi.Option;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcCompression;

/**
 * @author Egor Kuleshov
 */
public class ConnectionOptions {
    /**
     * GRPC compression
     */
    public static final Option<GrpcCompression> GRPC_COMPRESSION = Option.valueOf("grpcCompression");
    /**
     * Balancing policy
     */
    public static final Option<BalancingSettings.Policy> BALANCING_POLICY = Option.valueOf("balancingPolicy");
    /**
     * Use TLS connection
     */
    public static final Option<Boolean> SECURE_CONNECTION = Option.valueOf("secureConnection");
    /**
     * Use TLS connection with certificate from bytes
     */
    public static final Option<byte[]> SECURE_CONNECTION_CERTIFICATE = Option.valueOf("secureConnectionCertificate");
    /**
     * Use TLS connection with certificate from provided path
     */
    public static final Option<String> SECURE_CONNECTION_CERTIFICATE_FILE = Option.valueOf("secureConnectionCertificateFile");
    /**
     * Token-based authentication
     */
    public static final Option<String> TOKEN = Option.valueOf("token");
    /**
     * Service account file based authentication
     */
    public static final Option<String> SERVICE_ACCOUNT_FILE = Option.valueOf("saFile");
    /**
     * Use metadata service for authentication
     */
    public static final Option<Boolean> USE_METADATA = Option.valueOf("useMetadata");
}
