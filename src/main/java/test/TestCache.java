package test;

import crawler.LruCache;

public class TestCache {
    public static void main(String[] args) throws InterruptedException {
        // LRU 30 seconds
        System.out.println("test cache:");
        LruCache cache = LruCache.getInstance();
        Object obj = new Object();
        cache.put(obj.toString(), true);
        assert cache.get(obj.toString());
        Thread.sleep(10000);
        assert cache.get(obj.toString());
        Thread.sleep(10000);
        assert cache.get(obj.toString());
        Thread.sleep(5000);
        assert cache.get(obj.toString());
        Thread.sleep(10000);
        assert !cache.get(obj.toString());
        System.out.println("test ok!");
    }
}
