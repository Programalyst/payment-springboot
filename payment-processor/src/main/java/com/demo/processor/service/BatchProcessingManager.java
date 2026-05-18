package com.demo.processor.service;

import com.demo.processor.model.Transaction;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.*;

@Service // Service is a singleton by default
public class BatchProcessingManager {

    // Grouping queues per card
    private final ConcurrentHashMap<String, BlockingQueue<Transaction>> cardQueues = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    private ParallelBatchProcessor batchProcessor;

    public void queueTransaction(Transaction tx) {
        cardQueues.computeIfAbsent(tx.getCardId(), k -> {
            LinkedBlockingQueue<Transaction> queue = new LinkedBlockingQueue<>();
            // Schedule a flush for this card's bucket every 50 milliseconds
            scheduler.scheduleAtFixedRate(() -> flushBatch(tx.getCardId(), queue), 50, 50, TimeUnit.MILLISECONDS);
            return queue;
        }).add(tx);
    }

    private void flushBatch(String cardId, BlockingQueue<Transaction> queue) {
        if (queue.isEmpty()) return;

        List<Transaction> batch = new ArrayList<>();
        queue.drainTo(batch);

        if (!batch.isEmpty()) {
            // Process the batch for this specific card
            batchProcessor.processCardBatch(cardId, batch);
        }
    }
}