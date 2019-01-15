package com.king.learn.Flink.streaming.join;

import java.io.Serializable;
import java.util.Iterator;


/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class ThrottledIterator<T> implements Iterator<T>, Serializable {
    private static final long serialVersionUID = -694284704712549217L;

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final Iterator<T> source;

    private final long sleepBatchSize;
    private final long sleepBatchTime;
    private long lastBatchCheckTime;
    private long num;

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = source;
        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 100) {
            // 每50毫秒发射多少个元素
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            // 这个元素要多长时间
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException("'elements per second' must be positive and not zero");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        // delay if necessary
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;

                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;
                if (elapsed < sleepBatchTime) {
                    try {
                        Thread.sleep(sleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        // restore interrupt flag and proceed
                        Thread.currentThread().interrupt();
                    }
                }
                lastBatchCheckTime = now;
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }

        return source.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
