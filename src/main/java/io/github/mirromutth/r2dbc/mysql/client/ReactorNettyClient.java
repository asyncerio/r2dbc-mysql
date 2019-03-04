/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.backend.AbstractHandshakeMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessageDecoder;
import io.github.mirromutth.r2dbc.mysql.message.backend.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeV10Message;
import io.github.mirromutth.r2dbc.mysql.message.frontend.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private final AtomicReference<Connection> connection;

    private final BackendMessageDecoder messageDecoder;

    private final AtomicInteger sequenceId = new AtomicInteger();

    private final EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<FrontendMessage> requests = requestProcessor.sink();

    private final Queue<MonoSink<BackendMessage>> responseReceivers = Queues.<MonoSink<BackendMessage>>unbounded().get();

    // it will always not null if the client initialized, so do not add @Nullable
    private volatile MySqlSession session;

    private volatile boolean closed = false;

    private ReactorNettyClient(
        Connection connection,
        ConnectProperties connectProperties,
        MonoProcessor<MySqlSession> sessionProcessor
    ) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(sessionProcessor, "sessionProcessor must not be null");

        connection.addHandler(new EnvelopeDecoder())
            .addHandler(new SubscribersCompleteHandler(this.requestProcessor, this.responseReceivers));

        this.messageDecoder = new BackendMessageDecoder(this.sequenceId);
        this.connection = new AtomicReference<>(connection);

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleHandshake = handleBackendMessage(
            AbstractHandshakeMessage.class,
            (message, sink) -> {
                if (message instanceof HandshakeV10Message) {
                    HandshakeV10Message messageV10 = (HandshakeV10Message) message;
                    HandshakeHeader header = messageV10.getHandshakeHeader();

                    int serverCapabilities = messageV10.getServerCapabilities();

                    MySqlSession session = new MySqlSession(
                        header.getConnectionId(),
                        header.getServerVersion(),
                        serverCapabilities,
                        CharCollation.fromId(messageV10.getCollationLow8Bits() & 0xFF),
                        connectProperties.getDatabase(),
                        calculateCapabilities(serverCapabilities, connectProperties),
                        connectProperties.getUsername(),
                        connectProperties.getPassword(),
                        messageV10.getSalt(),
                        messageV10.getAuthType()
                    );

                    sessionProcessor.onNext(session);
                } else {
                    close().subscribe();
                }
            }
        );

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleError = handleBackendMessage(
            ErrorMessage.class,
            (message, sink) -> {
                logger.error("Error: error code {}, sql state: {}, message: {}", message.getErrorCode(), message.getSqlState(), message.getErrorMessage());
                sink.next(message);
            }
        );

        // TODO: implement receive
        Mono<Void> receive = connection.inbound().receive()
            .retain()
            .map(buf -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Inbound:\n{}", ByteBufUtil.prettyHexDump(buf));
                }
                return messageDecoder.decode(buf);
            })
            .doOnNext(message -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Response: {}", message);
                }
            })
            .handle(handleHandshake)
            .handle(handleError)
            .doOnNext(message -> {
                MonoSink<BackendMessage> receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success(message);
                }
            })
            .doOnComplete(() -> {
                MonoSink<BackendMessage> receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success();
                }
            })
            .then();

        Mono<Void> request = this.requestProcessor.doOnNext(message -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Request: {}", message);
            }
        }).concatMap(message -> connection.outbound().send(message.encode(connection.outbound().alloc(), this.sequenceId, this.session).doOnNext(buf -> {
            if (logger.isTraceEnabled()) {
                logger.trace("Outbound:\n{}", ByteBufUtil.prettyHexDump(buf));
            }
        }))).then();

        Flux.merge(receive, request)
            .doFinally(ignored -> this.messageDecoder.dispose())
            .onErrorResume(e -> {
                logger.error("Connection Error", e);
                return close();
            })
            .subscribe();
    }

    @Override
    public Mono<BackendMessage> exchange(FrontendMessage request) {
        requireNonNull(request, "request must not be null");

        return Mono.create(sink -> {
            if (this.closed) {
                sink.error(new IllegalStateException("Cannot exchange message because the connection is closed"));
            }

            synchronized (this) {
                this.responseReceivers.add(sink);
                this.requests.next(request);
            }
        });
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) { // client is closed or closing
                return Mono.empty();
            }

            return Mono.just(ExitMessage.getInstance())
                .doOnNext(message -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Request: {}", message);
                    }
                })
                .map(message -> connection.outbound().send(message.encode(connection.outbound().alloc(), sequenceId, session)))
                .then()
                .doOnSuccess(ignored -> connection.dispose())
                .then(connection.onDispose())
                .doOnSuccess(ignored -> this.closed = true);
        });
    }

    @Override
    public MySqlSession getSession() {
        return session;
    }

    private ReactorNettyClient initSession(MySqlSession session) {
        this.session = session;
        this.messageDecoder.initSession(session);

        return this;
    }

    static Mono<ReactorNettyClient> connect(ConnectProperties connectProperties) {
        return connect(ConnectionProvider.newConnection(), connectProperties);
    }

    private static Mono<ReactorNettyClient> connect(ConnectionProvider connectionProvider, ConnectProperties connectProperties) {
        requireNonNull(connectionProvider, "connectionProvider must not be null");
        requireNonNull(connectProperties, "connectProperties must not be null");

        TcpClient client = TcpClient.create(connectionProvider)
            .host(connectProperties.getHost())
            .port(connectProperties.getPort());

        MonoProcessor<MySqlSession> session = MonoProcessor.create(WaitStrategy.sleeping());

        return client.connect()
            .map(conn -> new ReactorNettyClient(conn, connectProperties, session))
            .flatMap(c -> session.map(c::initSession));
    }

    @SuppressWarnings("unchecked")
    private static <T extends BackendMessage> BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleBackendMessage(
        Class<T> type,
        BiConsumer<T, SynchronousSink<BackendMessage>> consumer
    ) {
        return (message, sink) -> {
            if (type.isInstance(message)) {
                consumer.accept((T) message, sink);
            } else {
                sink.next(message);
            }
        };
    }

    private static int calculateCapabilities(int serverCapabilities, ConnectProperties properties) {
        // use protocol 41 and deprecate EOF message
        int clientCapabilities = serverCapabilities | Capability.PROTOCOL_41.getFlag() | Capability.DEPRECATE_EOF.getFlag();

        if (!properties.isUseSsl()) {
            clientCapabilities &= ~Capability.SSL.getFlag();
        }

        if (properties.getDatabase().isEmpty()) {
            clientCapabilities &= ~Capability.CONNECT_WITH_DB.getFlag();
        }

        return clientCapabilities;
    }
}
