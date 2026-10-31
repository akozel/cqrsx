# Backlog epic 02: a load generator that stands in for k6 on the stand (perf/run.sh GENERATOR=wrk sets
# LOAD_K6_IMAGE to this image). wrk is C with LuaJIT and costs a small fraction of k6's CPU per request,
# so the generator stops being the ceiling. The entry point takes k6's arguments and environment and
# writes /home/k6/result.json in the shape of demo/load/k6/lib/summary.js.
FROM docker.io/library/alpine:3.22
RUN apk add --no-cache wrk jq && mkdir -p /home/k6
COPY gen.sh /usr/local/bin/gen
COPY scenarios/ /gen/
ENTRYPOINT ["/usr/local/bin/gen"]
