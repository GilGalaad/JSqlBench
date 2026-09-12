package engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerContext {

    private final ArrayList<Long> samples = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();

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

    public List<Long> allSamples() {
        return samplesFrom(0);
    }

}
