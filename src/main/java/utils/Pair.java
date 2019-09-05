package utils;

import java.util.Map;

public class Pair<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }

    public void setKeyVal(K key, V val) {
        this.key = key;
        this.value = val;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof Pair && this.key.equals(((Pair) object).key);
    }

}