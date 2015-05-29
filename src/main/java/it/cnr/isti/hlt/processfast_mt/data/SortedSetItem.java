package it.cnr.isti.hlt.processfast_mt.data;


import java.io.Serializable;

public class SortedSetItem<K extends Comparable & Serializable, V extends Serializable> implements Comparable<SortedSetItem<K, V>> {
    public SortedSetItem(K key, V item) {
        if (key == null) throw new NullPointerException("The key value is 'null'");
        if (item == null) throw new IllegalArgumentException("The item is 'null'");
        this.key = key;
        this.item = item;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SortedSetItem))
            return false;
        SortedSetItem si = (SortedSetItem) obj;
        return key.equals(si.key) && item.equals(si.item);
    }

    @Override
    public int compareTo(SortedSetItem o) {
        int ret = key.compareTo(o.key);
        if (ret == 0) ret = -1;
        return ret;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getItem() {
        return item;
    }

    public void setItem(V item) {
        this.item = item;
    }

    /**
     * The key used for comparison purposes.
     */
    private K key;
    /**
     * The associated stored item.
     */
    private V item;
}
