# Event-driven pull fetches

Pull consumers register a waiter before checking durable state. The waiter
arms its publication notification before each empty batch check, which covers
both race orders: a record committed before registration is found by the state
check, while a record committed after registration wakes the armed waiter.
Competing waiters re-check under the durable-state lock, so only one can create
an exclusive delivery lease.

Waiters are keyed by durable consumer and retain its filter subject. Durable
publication wakes only matching filters; acknowledgements, redelivery, and
credit changes wake the affected consumer. Consumer deletion, connection
disconnect, and broker shutdown cancel waits. Socket FETCH handling runs in a
separate task so the connection read loop can observe EOF and perform that
cancellation even during a long maximum wait.

The registry permits one outstanding FETCH per connection and at most 64 per
consumer. Exceeding either bound returns a protocol error. Waiters are removed
by an RAII guard on delivery, timeout, cancellation, or error.

There is no retry interval. An empty FETCH sleeps on notifications and its exact
deadline, without periodically locking durable state. Tests hold 16 idle
requests across the former polling interval and assert that the number of state
checks does not increase.
