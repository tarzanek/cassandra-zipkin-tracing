package com.scylladb.tracing;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.EmptySpanCollectorMetricsHandler;
import com.github.kristofa.brave.Sampler;
import com.github.kristofa.brave.ServerTracer;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.HttpSpanCollector;
import com.google.common.collect.ImmutableMap;
import com.thelastpickle.cassandra.tracing.ZipkinTraceState;
import com.thelastpickle.cassandra.tracing.ZipkinTracing;
import com.twitter.zipkin.gen.Span;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SystemTracesZipkinLoader extends Tracing {

    public static final String ZIPKIN_TRACE_HEADERS = "zipkin";

    static final int SAMPLE_RATE = 1;

    private static final Logger logger = LoggerFactory.getLogger(ZipkinTracing.class);

    private static final String HTTP_SPAN_COLLECTOR_HOST = System.getProperty("ZipkinTracing.httpCollectorHost", "localhost");
    private static final String HTTP_SPAN_COLLECTOR_PORT = System.getProperty("ZipkinTracing.httpCollectorPort", "9411");
    private static final String HTTP_COLLECTOR_URL = "http://" + HTTP_SPAN_COLLECTOR_HOST + ':' + HTTP_SPAN_COLLECTOR_PORT;

    private final SpanCollector spanCollector
            = HttpSpanCollector.create(HTTP_COLLECTOR_URL, new EmptySpanCollectorMetricsHandler());
    //= KafkaSpanCollector.create("127.0.0.1:9092", new EmptySpanCollectorMetricsHandler());

    private final Sampler SAMPLER = Sampler.ALWAYS_SAMPLE;

    private final AtomicMonotonicTimestampGenerator TIMESTAMP_GENERATOR = new AtomicMonotonicTimestampGenerator();

    volatile Brave brave = new Brave
            .Builder( "scylla:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getBroadcastAddress().getHostName())
            .spanCollector(spanCollector)
            .traceSampler(SAMPLER)
            .clock(() -> { return TIMESTAMP_GENERATOR.next(); })
            .build();

    ClientTracer getClientTracer()
    {
        return brave.clientTracer();
    }

    private ServerTracer getServerTracer()
    {
        return brave.serverTracer();
    }

    public SystemTracesZipkinLoader() {

    }

    public void stop() {

    }

    // defensive override, see CASSANDRA-11706
    @Override
    public UUID newSession(UUID sessionId, Map<String,ByteBuffer> customPayload)
    {
        return newSession(sessionId, Tracing.TraceType.QUERY, customPayload);
    }

    @Override
    protected UUID newSession(UUID sessionId, Tracing.TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        ByteBuffer bb = null != customPayload ? customPayload.get(ZIPKIN_TRACE_HEADERS) : null;
        if (null != bb)
        {
            if (isValidHeaderLength(bb.limit()))
            {
                extractAndSetSpan(bb.array(), traceType.name());
            }
            else
            {
                logger.error("invalid customPayload in {}", ZIPKIN_TRACE_HEADERS);
                getServerTracer().setStateUnknown(traceType.name());
            }
        }
        else
        {
            getServerTracer().setStateUnknown(traceType.name());
        }
        return super.newSession(sessionId, traceType, customPayload);
    }

    protected void stopSessionImpl()
    {
        ZipkinTraceState state = (ZipkinTraceState) get();
        if (state != null)
        {
            state.close();
            getServerTracer().setServerSend();
            getServerTracer().clearCurrentSpan();
        }
    }

    public void doneWithNonLocalSession(TraceState s)
    {
        ZipkinTraceState state = (ZipkinTraceState) s;
        state.close();
        getServerTracer().setServerSend();
        getServerTracer().clearCurrentSpan();
        super.doneWithNonLocalSession(state);
    }

    public TraceState begin(String request, InetAddress client, Map<String, String> parameters)
    {
        if (null != client)
            getServerTracer().submitBinaryAnnotation("client", client.toString());

        getServerTracer().submitBinaryAnnotation("request", request);
        return get();
    }

    public TraceState initializeFromMessage(final MessageIn<?> message)
    {
        byte [] bytes = message.parameters.get(ZIPKIN_TRACE_HEADERS);

        assert null == bytes || isValidHeaderLength(bytes.length)
                : "invalid customPayload in " + ZIPKIN_TRACE_HEADERS;

        if (null != bytes)
        {
            if (isValidHeaderLength(bytes.length))
            {
                extractAndSetSpan(bytes, message.getMessageType().name());
            }
            else
            {
                logger.error("invalid customPayload in {}", ZIPKIN_TRACE_HEADERS);
            }
        }
        return super.initializeFromMessage(message);
    }

    private void extractAndSetSpan(byte[] bytes, String name) {
        if (32 == bytes.length)
        {
            // Zipkin B3 propagation
            SpanId spanId = SpanId.fromBytes(bytes);
            getServerTracer().setStateCurrentTrace(spanId.traceId, spanId.spanId, spanId.parentId, name);
        }
        else
        {
            // deprecated aproach
            ByteBuffer bb = ByteBuffer.wrap(bytes);

            getServerTracer().setStateCurrentTrace(
                    bb.getLong(),
                    bb.getLong(),
                    24 <= bb.limit() ? bb.getLong() : null,
                    name);
        }
    }

    @Override
    public Map<String, byte[]> getTraceHeaders()
    {
        assert isTracing();
        Span span = brave.clientSpanThreadBinder().getCurrentClientSpan();

        SpanId spanId = SpanId.builder()
                .traceId(span.getTrace_id())
                .parentId(span.getParent_id())
                .spanId(span.getId())
                .build();

        return ImmutableMap.<String, byte[]>builder()
                .putAll(super.getTraceHeaders())
                .put(ZIPKIN_TRACE_HEADERS, spanId.bytes())
                .build();
    }

    @Override
    public void trace(final ByteBuffer sessionId, final String message, final int ttl)
    {
        UUID sessionUuid = UUIDGen.getUUID(sessionId);
        TraceState state = Tracing.instance.get(sessionUuid);
        state.trace(message);
    }

    protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType)
    {
        getServerTracer().setServerReceived();
        getServerTracer().submitBinaryAnnotation("sessionId", sessionId.toString());
        getServerTracer().submitBinaryAnnotation("coordinator", coordinator.toString());
        getServerTracer().submitBinaryAnnotation("started_at", Instant.now().toString());

        return new ZipkinTraceState(
                brave,
                coordinator,
                sessionId,
                traceType,
                brave.serverSpanThreadBinder().getCurrentServerSpan());
    }

    private static boolean isValidHeaderLength(int length)
    {
        return 16 == length || 24 == length || 32 == length;
    }

    static Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
    static Session session = cluster.connect("system_traces");

    static PreparedStatement selectEvents = session.prepare("SELECT * FROM system_traces.events where session_id=?");

    public static void selectSessions(Tracing tracing) {
        System.out.print("\n\nFetching sessions ...");
        ResultSet results = session.execute("SELECT * FROM system_traces.sessions");
        Map payload=new HashMap<String, ByteBuffer>();
        for (Row row : results) {
//  session_id | client | command | coordinator | duration | parameters | request | request_size | response_size | started_at
            UUID session_id = row.getUUID("session_id");
            String command = row.getString("command");
            InetAddress client = row.getInet("client");
            if (command != null && command.isEmpty()) {
                payload.put(ZIPKIN_TRACE_HEADERS, command);
            } else {
                payload = new HashMap<String, ByteBuffer>();
            }
            tracing.newSession(session_id,payload);
            TraceState trace = null;
            trace = tracing.begin(command,client,new HashMap<String,String>());
            selectEvents(session_id,trace, tracing);
            tracing.stopSession();
        }
    }

    public static void selectEvents(UUID sessionId, TraceState state, Tracing tracing) {
        System.out.print("\n\nFetching events for session: "+sessionId+"  ...");
        ResultSet results = session.execute(selectEvents.bind(sessionId));
        for (Row row : results) {
//   session_id | event_id | activity | source | scylla_parent_id | scylla_span_id | source_elapsed | thread
            String activity = row.getString("activity");
            String thread = row.getString("thread");
            ((ZipkinTraceState)state).traceImplWithClientSpans(activity);
        }
        tracing.doneWithNonLocalSession(state);
    }

    //Indexes:

    // sessions_time_idx
    // node_slow_log_time_idx
    // minute | started_at | session_id | start_time | node_ip | shard
// SELECT * from system_traces.sessions_time_idx where minutes in ('2016-09-07 16:56:00-0700') and started_at > '2016-09-07 16:56:30-0700';

// system_traces.node_slow_log table ???
    // start_time | date | node_ip | shard | command | duration | parameters | session_id | source ip | table_names | username

    public static void main(String[] args)  {


        Tracing tracing = new SystemTracesZipkinLoader();
        selectSessions(tracing);
        cluster.close();

        tracing.stopSession();
        SystemTracesZipkinLoader caster = (SystemTracesZipkinLoader)tracing;
        caster.stop();

        System.exit(0);

    }

}
