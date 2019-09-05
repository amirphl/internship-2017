package crawler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;
import utils.Constants;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LruCache {
    private LoadingCache<String, Boolean> cacheLoader;
    private static LruCache myCache;

    public synchronized static LruCache getInstance() {
        if (myCache == null) {
            myCache = new LruCache();
        }
        return myCache;
    }

    private LruCache() {
        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(Constants.LRU_TIME_LIMIT, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(@NotNull String key) {
                        return Boolean.TRUE;
                    }
                });
    }

    Boolean exist(String url) {
        return cacheLoader.getIfPresent(url) != null;
    }

    public synchronized boolean get(String url) {
        try {
            return cacheLoader.get(url);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    public synchronized void put(String key, Boolean value) {
        cacheLoader.put(key, value);
    }
}
