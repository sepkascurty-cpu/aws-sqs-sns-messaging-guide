#!/usr/bin/env python3
"""
AWS SQS & SNS Messaging Demo — fan-out + worker with DLQ redrive.

Run offline (no AWS account needed):
    python messaging.py

Run against real AWS (needs boto3 + credentials):
    python messaging.py --live

What it demonstrates:
  1. SNS topic fan-out: one publish -> a standard queue AND a FIFO queue.
  2. Worker loop: long polling, batch receive, delete-after-success.
  3. Visibility timeout: a crashed worker's message reappears automatically.
  4. Dead-letter queue: messages failing 3 receives move to the DLQ,
     then get redriven back to the source queue.
"""

import argparse
import json
import sys
import time
import uuid
from collections import deque

# ---------------------------------------------------------------------------
# Offline simulator (in-memory SNS + SQS) — same API shape as boto3
# ---------------------------------------------------------------------------


class SimulatedMessage:
    def __init__(self, body, group_id=None, dedup_id=None):
        self.body = body
        self.group_id = group_id
        self.dedup_id = dedup_id
        self.receive_count = 0
        self.visible_at = 0.0
        self.receipt_handle = str(uuid.uuid4())


class SimulatedQueue:
    """Minimal SQS: visibility timeout, maxReceiveCount -> DLQ, redrive."""

    def __init__(self, name, fifo=False, max_receive_count=3,
                 visibility_timeout=30, dlq=None):
        self.name = name
        self.fifo = fifo
        self.max_receive_count = max_receive_count
        self.visibility_timeout = visibility_timeout
        self.dlq = dlq
        self._messages = deque()
        self._inflight = {}
        self._seen_dedup = set()

    def send(self, body, group_id=None, dedup_id=None):
        if self.fifo and dedup_id in self._seen_dedup:
            print(f"  [FIFO dedup] duplicate ignored: {dedup_id}")
            return None
        if dedup_id:
            self._seen_dedup.add(dedup_id)
        msg = SimulatedMessage(body, group_id, dedup_id)
        self._messages.append(msg)
        return msg

    def _sweep_inflight(self):
        """Return expired leases to the queue; exhausted messages go to DLQ."""
        now = time.time()
        for handle, msg in list(self._inflight.items()):
            if msg.visible_at > now:
                continue
            del self._inflight[handle]
            if msg.receive_count >= self.max_receive_count and self.dlq:
                self.dlq.send(msg.body, msg.group_id, msg.dedup_id)
                print(f"  [DLQ] moved to {self.dlq.name} after "
                      f"{msg.receive_count} receives: {msg.body[:60]}")
            else:
                self._messages.append(msg)

    def receive(self, max_number=10):
        self._sweep_inflight()
        now = time.time()
        visible = [m for m in self._messages if m.visible_at <= now]
        batch = visible[:max_number]
        for msg in batch:
            self._messages.remove(msg)
            msg.receive_count += 1
            msg.visible_at = now + self.visibility_timeout
            self._inflight[msg.receipt_handle] = msg
        return batch

    def delete(self, receipt_handle):
        self._inflight.pop(receipt_handle, None)

    def depth(self):
        return len(self._messages) + len(self._inflight)


class SimulatedTopic:
    """Minimal SNS: publish -> every subscribed queue gets a copy."""

    def __init__(self, name):
        self.name = name
        self.subscriptions = []

    def subscribe(self, queue):
        self.subscriptions.append(queue)

    def publish(self, message):
        for queue in self.subscriptions:
            queue.send(message)
        print(f"  [SNS] '{self.name}' fanned out to "
              f"{len(self.subscriptions)} queue(s)")


# ---------------------------------------------------------------------------
# Demo flow (works identically against the simulator or real boto3)
# ---------------------------------------------------------------------------


def run_simulator_demo():
    print("== Offline simulator demo (no AWS credentials needed) ==\n")

    dlq = SimulatedQueue("orders-dlq")
    orders_q = SimulatedQueue("orders-queue", max_receive_count=3,
                              visibility_timeout=5, dlq=dlq)
    payments_q = SimulatedQueue("payments-queue.fifo", fifo=True,
                                max_receive_count=3,
                                visibility_timeout=5, dlq=dlq)
    topic = SimulatedTopic("orders-events")
    topic.subscribe(orders_q)
    topic.subscribe(payments_q)

    # 1. Producer publishes ONE order event.
    order = {"order_id": "ORD-1001", "customer": "C-42",
             "total": 129_000, "currency": "IDR"}
    print("1) Producer publishes one order event to SNS:")
    topic.publish(json.dumps(order))
    print(f"   orders-queue depth: {orders_q.depth()}, "
          f"payments-queue depth: {payments_q.depth()}\n")

    # 2. FIFO dedup: publishing the same payment twice is ignored.
    print("2) FIFO dedup — same payment published twice:")
    payments_q.send(json.dumps(order), group_id="C-42",
                    dedup_id="pay-ORD-1001")
    payments_q.send(json.dumps(order), group_id="C-42",
                    dedup_id="pay-ORD-1001")
    print(f"   payments-queue depth: {payments_q.depth()}\n")

    # 3. Worker processes the standard queue successfully.
    print("3) Worker processes orders-queue (success -> delete):")
    for msg in orders_q.receive():
        print(f"   processing: {msg.body[:60]}")
        orders_q.delete(msg.receipt_handle)
    print(f"   orders-queue depth: {orders_q.depth()}\n")

    # 4. Worker crashes mid-processing -> message reappears, then DLQ.
    print("4) Poison message: worker crashes 3x, then DLQ takes it:")
    print("   (first, the good payments are processed normally)")
    for msg in payments_q.receive():
        print(f"   processed payment: {msg.body[:60]}")
        payments_q.delete(msg.receipt_handle)
    payments_q.send(json.dumps({"order_id": "ORD-9999", "poison": True}),
                    group_id="C-99", dedup_id="pay-ORD-9999")
    for attempt in range(1, 5):
        batch = payments_q.receive()
        if not batch:
            print("   (nothing visible — the poison message is in the DLQ)")
            break
        for msg in batch:
            print(f"   attempt {attempt}: worker crashed, "
                  f"no delete (receive #{msg.receive_count})")
            # Simulate the visibility timeout expiring instantly
            # so the next loop can pick the message up again.
            msg.visible_at = 0.0
    print(f"   DLQ depth: {dlq.depth()}\n")

    # 5. Redrive: replay DLQ messages after the bug is fixed.
    print("5) Bug fixed — redrive DLQ back to the source queue:")
    while dlq._messages:
        msg = dlq._messages.popleft()
        msg.receive_count = 0
        # Fresh dedup ID: the original one is still inside the 5-min
        # dedup window, so reusing it would drop the redriven message.
        payments_q.send(msg.body, msg.group_id,
                        msg.dedup_id + "-redrive")
    print(f"   DLQ depth: {dlq.depth()}, "
          f"payments-queue depth: {payments_q.depth()}")
    print("\nDemo complete. Key takeaway: the queue, not the worker, "
          "owns reliability.")


def run_live_demo():
    import boto3  # noqa: PLC0415  (only needed for --live)

    print("== Live AWS demo ==")
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    topic = sns.create_topic(Name="orders-events")["TopicArn"]
    q_url = sqs.create_queue(QueueName="orders-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=q_arn)

    sns.publish(TopicArn=topic, Message=json.dumps(
        {"order_id": "ORD-1001", "total": 129_000}))
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10,
                               WaitTimeSeconds=5).get("Messages", [])
    for m in msgs:
        print("processing:", m["Body"][:80])
        sqs.delete_message(QueueUrl=q_url,
                           ReceiptHandle=m["ReceiptHandle"])
    print("Live demo complete.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true",
                        help="run against real AWS instead of the simulator")
    args = parser.parse_args()
    if args.live:
        run_live_demo()
    else:
        run_simulator_demo()


if __name__ == "__main__":
    sys.exit(main())
