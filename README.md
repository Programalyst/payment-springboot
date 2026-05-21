```text
========================================================================
                  PAYMENT TRANSACTIONS BATCHING ENGINE
========================================================================

    [ 3,000 Simultaneous HTTP Requests ]
                     │
                     │  (Async Ingestion via RestController)
                     ▼
┌────────────────────────────────────────────────────────┐
│  1. BATCH PROCESSING MANAGER (The In-Memory Buffer)    │
├────────────────────────────────────────────────────────┤
│  - Accepts payments and instantly returns HTTP 202     │
│  - Isolates accounts inside a ConcurrentHashMap        │
│  - Groups items: Map<CardID, BlockingQueue>            │
└────────────────────┬───────────────────────────────────┘
                     │
                     │  (Automated Clock Trigger: Every 50ms)
                     ▼
┌────────────────────────────────────────────────────────┐
│  2. PARALLEL BATCH PROCESSOR (The Execution Engine)    │
├────────────────────────────────────────────────────────┤
│  - Uses queue.drainTo() to empty buffer atomically     │
│  - Queries DB exactly ONCE for Balance + Version       │
│  - Runs validations and deducts metrics locally        │
└────────────────────┬───────────────────────────────────┘
                     │
                     │  (Optimistic Concurrency Control Check)
                     ▼
           /───────────────────\
          <  Version Match DB?  >
           \───────────────────/
             /               \
    [YES]   /                 \   [NO] (Collision Detected)
           /                   \
          ▼                     ▼
┌────────────────────┐   ┌────────────────────┐
│ COMMIT TO DATABASE │   │   RETRY BATCH      │
│ - Increment Version│   │ - Fetch New Balance│
│ - Stream to Kafka  │   │ - Re-run Logic     │
└────────────────────┘   └────────────────────┘
```


### Step 1: Ingestion & Grouping (BatchProcessingManager)

When the PaymentController receives a payment, it does not call the database. It immediately hands the transaction over to the BatchProcessingManager and returns an HTTP 202 Accepted response to the client. This keeps the API response times under 5 milliseconds.

**Inside BatchProcessingManager:**

- The Routing Map: It maintains a ConcurrentHashMap<String, BlockingQueue<Transaction>>. The key is the cardId.

- Dynamic Queue Creation: If Card 123 sends its first transaction, the manager creates a dedicated queue for Card 123 on the fly using .computeIfAbsent().

- The Clock Trigger: The moment that queue is born, a ScheduledExecutorService spawns a background timer specifically for Card 123. It is configured to fire a "flush" command every 50 milliseconds.

If 3,000 transactions for Card 123 hit the API within that 50ms window, they are all safely piled into Card 123's isolated queue.


### Step 2: The Hand-Off (flushBatch)

Every 50ms, the background scheduler wakes up and executes flushBatch() for that specific card:

1. It checks if the queue has transactions.

2. If yes, it uses queue.drainTo(batch) to atomically pull all accumulated transactions out of the queue and dump them into a standard List<Transaction>.

3. It immediately calls batchProcessor.processCardBatch(cardId, batch).
   

### Step 3: Execution & Concurrency Check (ParallelBatchProcessor)

Now, the ParallelBatchProcessor takes that list of transactions for Card 123 and processes them in isolation. 

1. **Single Database Read:** It queries the mock database exactly once to get the current balance and the current schema version (e.g., Balance: $5000, Version: 1).

2. **In-Memory Ledger Loop:** It loops through all transactions in the batch locally in memory. It applies singleton validators, deducts the amounts, and updates the state of each transaction object to APPROVED or DECLINED.

3. **The Optimistic Concurrency Write (OCC):** Once the loop is done, it attempts to write the final balance back to the database using computeIfPresent(). It checks: "Is the database version still 1?"
   - If YES (Success): It saves the new balance, changes the version to 2, and streams all 3,000 processed events to Kafka in one go.
   - If NO (Collision): If another server instance updated Card 123 while this batch was calculating, the version will be different. The processor rejects the write, fetches the brand-new database balance, and automatically retries the entire batch against the fresh data.