-- Test 2a, announce storm, for wrk: every request announces a brand-new concert (the id is new, the
-- body is the same as demo/load/k6/lib/api.js sends). Each thread counts the statuses it saw; done()
-- prints one `GEN_RESULT {json}` line that gen.sh turns into result.json.
local threads = {}
local counter = 0
local prefix
-- A global, not a local: done() reads it from every thread with thread:get, which sees globals only.
statuses = {}
local body = '{"type":"AnnounceConcert","artist":"Load Test","city":"Loadville","date":"2026-12-31"}'
local headers = { ["Content-Type"] = "application/json" }

function setup(thread)
  thread:set("id", #threads + 1)
  table.insert(threads, thread)
end

function init(args)
  -- ConcertId is [a-z0-9-]{1,64}: run slug (<= 24) + phase + thread + counter stays well below.
  local phase = os.getenv("PHASE") == "warmup" and "w" or "m"
  prefix = string.format("%s-%s%d-", os.getenv("RUN") or "run", phase, id)
end

function request()
  counter = counter + 1
  return wrk.format("POST", "/api/concerts/" .. prefix .. counter .. "/commands", headers, body)
end

function response(status, response_headers, response_body)
  statuses[status] = (statuses[status] or 0) + 1
end

function done(summary, latency, requests)
  local total = {}
  for _, thread in ipairs(threads) do
    for status, n in pairs(thread:get("statuses")) do
      total[status] = (total[status] or 0) + n
    end
  end
  local parts = {}
  for status, n in pairs(total) do
    table.insert(parts, string.format('"%d":%d', status, n))
  end
  local e = summary.errors
  io.write(string.format(
    'GEN_RESULT {"requests":%d,"duration_us":%d,"statuses":{%s},"errors":{"connect":%d,"read":%d,"write":%d,"status":%d,"timeout":%d},' ..
      '"latency_us":{"mean":%.1f,"p50":%d,"p95":%d,"p99":%d,"max":%d}}\n',
    summary.requests, summary.duration, table.concat(parts, ","), e.connect, e.read, e.write, e.status, e.timeout,
    latency.mean, latency:percentile(50), latency:percentile(95), latency:percentile(99), latency.max))
end
