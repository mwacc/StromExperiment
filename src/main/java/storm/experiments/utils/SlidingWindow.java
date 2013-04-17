package storm.experiments.utils;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SlidingWindow implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int windowLengthInSeconds;
    private final int maxObjectsInWindow;
    private final Map<UniqueKey, AtomicInteger> mapOfValues = new HashMap<UniqueKey, AtomicInteger>();

    private long initAt = 0; // when sliding window was initialized
    private int objectCounter = 0;

    public SlidingWindow(int windowLength, int maxObjectsInWindow) {
        System.out.println(String.format("Configure SlidingWindow, time %d, objects %d", windowLength, maxObjectsInWindow));
        this.windowLengthInSeconds = windowLength;
        this.maxObjectsInWindow = maxObjectsInWindow;
        initAt = System.currentTimeMillis();
    }

    public boolean isReady() {
        System.out.println( String.format("SlidingWindow, time %d, objects %d", (System.currentTimeMillis() - initAt), objectCounter) );
        if( System.currentTimeMillis() - initAt > windowLengthInSeconds ) return true;
        if( objectCounter > maxObjectsInWindow ) return true;
        return false;
    }

    public void emitTuple(Tuple tuple) {
        ++objectCounter;
        UniqueKey key = new UniqueKey(tuple.getString(1), tuple.getString(0));
        AtomicInteger v = mapOfValues.get(key);
        if(v == null) {
            v = new AtomicInteger(1);
        } else {
            v.incrementAndGet();
        }
        mapOfValues.put(key, v);
    }

    public Map<UniqueKey, AtomicInteger> getContent() {
        return Collections.unmodifiableMap(mapOfValues);
    }

    public void reset() {
        mapOfValues.clear();
        initAt = System.currentTimeMillis();
        objectCounter = 0;
    }

    public class UniqueKey {
        private String country;
        private String url;

        UniqueKey(String country, String url) {
            this.country = country;
            this.url = url;
        }

        public String getCountry() {
            return country;
        }

        public String getUrl() {
            return url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UniqueKey uniqueKey = (UniqueKey) o;

            if (country != null ? !country.equals(uniqueKey.country) : uniqueKey.country != null) return false;
            if (url != null ? !url.equals(uniqueKey.url) : uniqueKey.url != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = country != null ? country.hashCode() : 0;
            result = 31 * result + (url != null ? url.hashCode() : 0);
            return result;
        }
    }


}