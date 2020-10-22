# A Zipkin tracing plugin for Scylla

## Prerequisites
You need
 - running Zipkin
(e.g. `podman run -d -p 9411:9411 openzipkin/zipkin`)
 - running Scylla (also using podman?), with some data in system_traces (e.g. 
`nodetool settraceprobability 1 ; cqlsh -e "select * from system.local;" nodetool settraceprobability 0` )

Validate if there is any data with:
 
`cqlsh -e "select * from system_traces.sessions;"`

## Configuration (hardcoded :-( )
Open this loader directory with Jetbrains Idea (I used latest available in October 2020)
(you need also (open)jdk 1.8 and recent Idea with maven support)

edit in SystemTracesZipkinLoader.java
HTTP_SPAN_COLLECTOR_HOST
(and point to your Zipkin server hostname/IP, if other than localhost)

and ev.
cluster variable (so it won't try to connect to localhost Scylla, if you run Scylla elsewhere)

then adjust 
src/main/resources/cassandra.yaml
so suit your needs (Cluster Name is taken from there, also IP address)
(this needs to be fixed!)

## Running
after Idea imports the file, you should be able to just run the class SystemTracesZipkinLoader
(when you open it, just from Run menu use run and use "SystemTracesZipkinLoader.main()")

Idea will automatically add debug config with above run config, so then you can just use 
breakpoints, watches, execute/evaluate expression, stepping and all the debug goodness

## Checking results:
Go to Zipkin(if localhost): http://localhost:9411/zipkin/?lookback=1h&limit=10
and check your traces were loaded

## Hacking
methods
`selectSessions`
and
`selectEvents`
create using the ZIPKIN_TRACE_HEADERS the appropriate messages sent to Zipkin
they need to be enriched with what we want

## Cleanups:
Scylla:

Quick cleanup of :
cqlsh -e "truncate system_traces.sessions;"
cqlsh -e "truncate system_traces.events;"

Zipkin:
???

