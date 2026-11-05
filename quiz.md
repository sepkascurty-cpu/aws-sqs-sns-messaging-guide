# Quiz — AWS SQS & SNS Messaging

Test what you learned from the guide. 5 questions, answers collapsed below each one.

---

## 1. What is the fundamental difference between SQS and SNS?

> SQS is a **pull** queue (one message → one consumer, workers poll when ready).
> SNS is a **push** topic (one message → many subscribers, delivered immediately).
> Combine them for fan-out: SNS → N SQS queues, each consumer gets its own copy.

<details>
<summary>Answer</summary>

**SQS = queue (pull, 1-to-1), SNS = topic (push, 1-to-many).** SQS holds messages until a worker asks for them; SNS delivers instantly to all subscribers. The classic pattern is SNS fan-out into multiple SQS queues so each consumer processes at its own pace.

</details>

---

## 2. A standard SQS queue delivers your messages in the exact order they were sent. True or false?

<details>
<summary>Answer</summary>

**False.** Standard queues offer best-effort ordering — messages can arrive out of order, and at-least-once delivery means duplicates are possible. If strict order matters, use a **FIFO queue** (`.fifo` suffix) with a message-group ID.

</details>

---

## 3. Your worker receives a message, then crashes before deleting it. What happens?

<details>
<summary>Answer</summary>

The message stays invisible only for the **visibility timeout**. Once it expires, the message **reappears** in the queue and another worker picks it up. That is why processing must be **idempotent** — the same message can legitimately be processed twice. If the message keeps failing, after `maxReceiveCount` receives it is moved to the **dead-letter queue**.

</details>

---

## 4. When should you choose a FIFO queue over a standard queue?

<details>
<summary>Answer</summary>

Only when **ordering or exactly-once delivery is a real requirement** — e.g. payments, ledger updates, or any flow where a duplicate or a reorder costs money. FIFO caps throughput (~3,000 msg/s with batching) and requires the `.fifo` suffix plus message-group IDs. Default to Standard otherwise.

</details>

---

## 5. A single "poison" message keeps crashing your worker. Without a DLQ, what goes wrong — and what does a DLQ change?

<details>
<summary>Answer</summary>

Without a DLQ the poison message is received, crashes the worker, reappears after the visibility timeout, and is received again — **forever**, wedging the queue and burning compute. With a DLQ configured (`maxReceiveCount`, e.g. 3), the message is automatically moved aside after N failures, the queue keeps flowing, and you can **redrive** it back later once the bug is fixed. Alarm on DLQ depth > 0 so you notice immediately.

</details>

---

## Score yourself

| Correct | Level |
|---|---|
| 5 | Messaging pro — go decouple something |
| 3–4 | Solid — re-read the visibility timeout + FIFO sections |
| 0–2 | Re-read the guide, then run `python messaging.py` and watch the DLQ catch the poison message |
