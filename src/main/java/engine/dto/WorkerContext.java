package engine.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public record WorkerContext(int workerId, ArrayList<Long> samples, ReentrantLock lock) {

    public WorkerContext(int workerId) {
        this(workerId, new ArrayList<>(), new ReentrantLock());
    }

    public void addSample(long elapsedNanos) {
        lock.lock();
        try {
            samples.add(elapsedNanos);
        } finally {
            lock.unlock();
        }
    }

    public List<Long> samplesFrom(int fromIndex) {
        lock.lock();
        try {
            return new ArrayList<>(samples.subList(fromIndex, samples.size()));
        } finally {
            lock.unlock();
        }
    }

}
