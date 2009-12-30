/* CCSTM - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SnapHashMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K,V> {

    private static final int LOG_BF = 4;
    private static final int BF = 1 << LOG_BF;
    private static final int BF_MASK = BF - 1;
    private static final int maxLoad(final int capacity) { return capacity - (capacity >> 2); /* 0.75 */ }
    private static final int minLoad(final int capacity) { return (capacity >> 2); /* 0.25 */ }

    static class Generation {
    }

    static class HashEntry<K,V> {
        final Generation gen;
        final K key;
        final int hash;
        V value;
        final HashEntry<K,V> next;

        HashEntry(final Generation gen, final K key, final int hash, final V value, final HashEntry<K,V> next) {
            this.gen = gen;
            this.key = key;
            this.hash = hash;
            this.value = value;
            this.next = next;
        }

        HashEntry<K,V> withRemoved(final Generation gen, final HashEntry<K,V> target) {
            if (this == target) {
                return next;
            } else {
                return new HashEntry<K,V>(gen, key, hash, value, next.withRemoved(gen, target));
            }
        }
    }

    /** A small hash table.  The caller is responsible for synchronization in most
     *  cases.
     */
    static class LeafMap<K,V> {
        static final int MIN_CAPACITY = 8;
        static final int MAX_CAPACITY = MIN_CAPACITY * BF;

        /** Set to null when this LeafMap is split into pieces. */
        Generation gen;
        HashEntry<K,V>[] table;

        /** The number of unique hash codes recorded in this LeafMap.  This is also
         *  used to establish synchronization order, by reading in containsKey and
         *  get and writing in any updating function.  We track unique hash codes
         *  instead of entries because LeafMaps are split into multiple LeafMaps
         *  when they grow too large.  If we used a basic count, then if many keys
         *  were present with the same hash code this splitting operation would not
         *  help to restore the splitting condition.
         */
        volatile int uniq;

        @SuppressWarnings("unchecked")
        LeafMap(final Generation gen) {
            this.gen = gen;
            this.table = (HashEntry<K,V>[]) new HashEntry[MIN_CAPACITY];
            this.uniq = 0;
        }

        private LeafMap(final Generation gen, final LeafMap src) {
            this.gen = gen;
            this.table = (HashEntry<K,V>[]) src.table.clone();
            this.uniq = src.uniq;
        }

        LeafMap cloneForWriteL(final Generation gen) {
            return new LeafMap(gen, this);
        }

        boolean hasSplitL() {
            return gen == null;
        }

        boolean containsKeyU(final K key, final int hash) {
            if (uniq == 0) { // volatile read
                return false;
            }
            HashEntry<K,V> e = table[hash & (table.length - 1)];
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    return true;
                }
                e = e.next;
            }
            return false;
        }

        private synchronized V lockedReadValue(final HashEntry<K,V> e) {
            return e.value;
        }

        /** This is only valid for a quiesced map. */
        boolean containsValueQ(final Object value) {
            for (HashEntry<K,V> head : table) {
                HashEntry<K,V> e = head;
                while (e != null) {
                    V v = e.value;
                    if (v == null) {
                        v = lockedReadValue(e);
                    }
                    if (value.equals(v)) {
                        return true;
                    }
                    e = e.next;
                }
            }
            return false;
        }

        V getU(final K key, final int hash) {
            if (uniq == 0) { // volatile read
                return null;
            }
            HashEntry<K,V> e = table[hash & (table.length - 1)];
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    final V v = e.value;
                    if (v == null) {
                        return lockedReadValue(e);
                    }
                    return v;
                }
                e = e.next;
            }
            return null;
        }

        private void growIfNecessaryL() {
            assert(!hasSplitL());
            final int n = table.length;
            if (n < MAX_CAPACITY && uniq > maxLoad(n)) {
                rehashL(n << 1);
            }
        }

        private void shrinkIfNecessaryL() {
            assert(!hasSplitL());
            final int n = table.length;
            if (n > MIN_CAPACITY && uniq < minLoad(n)) {
                rehashL(n >> 1);
            }
        }

        @SuppressWarnings("unchecked")
        private void rehashL(final int newSize) {
            final HashEntry<K,V>[] prevTable = table;
            table = (HashEntry<K,V>[]) new HashEntry[newSize];
            for (HashEntry<K,V> head : prevTable) {
                reputAllL(head);
            }
        }

        private void reputAllL(final HashEntry<K,V> head) {
            if (head != null) {
                reputAllL(head.next);
                reputL(head);
            }
        }

        private void reputL(final HashEntry<K,V> e) {
            final int i = e.hash & (table.length - 1);
            final HashEntry<K,V> next = table[i];
            if (e.next == next) {
                // no new entry needed
                table[i] = e;
            } else {
                table[i] = new HashEntry<K,V>(gen, e.key, e.hash, e.value, next);
            }
        }

        V putL(final K key, final int hash, final V value) {
            growIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int insDelta = 1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final V prev = e.value;
                        if (e.gen == gen) {
                            // we have permission to mutate the node
                            e.value = value;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(gen, key, hash, value, head.withRemoved(gen, e));
                        }
                        uniq = uniq; // volatile store
                        return prev;
                    }
                    // Hash match, but not a key match.  If we eventually insert,
                    // then we won't modify uniq.
                    insDelta = 0;
                }
                e = e.next;
            }
            // no match
            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
            uniq += insDelta; // volatile store
            return null;
        }

        V removeL(final K key, final int hash) {
            shrinkIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int delDelta = -1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final HashEntry<K,V> target = e;

                        // continue the loop to get the right delDelta
                        if (delDelta != 0) {
                            e = e.next;
                            while (e != null) {
                                if (e.hash == hash) {
                                    delDelta = 0;
                                    break;
                                }
                                e = e.next;
                            }
                        }

                        // match
                        uniq += delDelta; // volatile store
                        table[i] = head.withRemoved(gen, target);
                        return target.value;
                    }
                    // hash match, but not key match
                    delDelta = 0;
                }
                e = e.next;
            }
            // no match
            return null;
        }

        //////// CAS-like

        V putIfAbsentL(final K key, final int hash, final V value) {
            growIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int insDelta = 1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match => failure
                        return e.value;
                    }
                    // Hash match, but not a key match.  If we eventually insert,
                    // then we won't modify uniq.
                    insDelta = 0;
                }
                e = e.next;
            }
            // no match
            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
            uniq += insDelta; // volatile store
            return null;
        }

        boolean replaceL(final K key, final int hash, final V oldValue, final V newValue) {
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    // key match
                    if (oldValue.equals(e.value)) {
                        // CAS success
                        if (e.gen == gen) {
                            // we have permission to mutate the node
                            e.value = newValue;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(gen, key, hash, newValue, head.withRemoved(gen, e));
                        }
                        uniq = uniq; // volatile store
                        return true;
                    } else {
                        // CAS failure
                        return false;
                    }
                }
                e = e.next;
            }
            // no match
            return false;
        }

        V replaceL(final K key, final int hash, final V value) {
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    // match
                    final V prev = e.value;
                    if (e.gen == gen) {
                        // we have permission to mutate the node
                        e.value = value;
                    } else {
                        // we must replace the node
                        table[i] = new HashEntry<K,V>(gen, key, hash, value, head.withRemoved(gen, e));
                    }
                    uniq = uniq; // volatile store
                    return prev;
                }
                e = e.next;
            }
            // no match
            return null;
        }

        boolean removeL(final K key, final int hash, final V value) {
            shrinkIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int delDelta = -1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // key match
                        if (!value.equals(e.value)) {
                            // CAS failure
                            return false;
                        }

                        final HashEntry<K,V> target = e;

                        // continue the loop to get the right delDelta
                        if (delDelta != 0) {
                            e = e.next;
                            while (e != null) {
                                if (e.hash == hash) {
                                    delDelta = 0;
                                    break;
                                }
                                e = e.next;
                            }
                        }

                        // match
                        uniq += delDelta; // volatile store
                        table[i] = head.withRemoved(gen, target);
                        return true;
                    }
                    // hash match, but not key match
                    delDelta = 0;
                }
                e = e.next;
            }
            // no match
            return false;
        }

        //////// Leaf splitting

        boolean shouldSplitL() {
            return uniq > maxLoad(MAX_CAPACITY);
        }

        @SuppressWarnings("unchecked")
        LeafMap<K,V>[] splitL(final int shift) {
            assert(!hasSplitL());

            final LeafMap<K,V>[] pieces = (LeafMap<K,V>[]) new LeafMap[BF];
            for (int i = 0; i < pieces.length; ++i) {
                pieces[i] = new LeafMap<K,V>(gen);
            }
            for (HashEntry<K,V> head : table) {
                scatterAllL(pieces, shift, head);
            }

            gen = null; // this marks us as split

            return pieces;
        }

        private static <K,V> void scatterAllL(final LeafMap<K,V>[] pieces, final int shift, final HashEntry<K,V> head) {
            if (head != null) {
                scatterAllL(pieces, shift, head.next);
                pieces[(head.hash >> shift) & (pieces.length - 1)].putL(head);
            }
        }

        private void putL(final HashEntry<K,V> entry) {
            growIfNecessaryL();
            final int i = entry.hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];

            // is this hash a duplicate?
            HashEntry<K,V> e = head;
            while (e != null && e.hash != entry.hash) {
                e = e.next;
            }
            if (e == null) {
                ++uniq;
            }

            if (e.next == head) {
                // no new entry needed
                table[i] = e;
            } else {
                table[i] = new HashEntry<K,V>(gen, e.key, e.hash, e.value, head);
            }
        }
    }

    static class BranchMap<K,V> extends AtomicReferenceArray<Object> {
        final Generation gen;
        final int shift;

        BranchMap(final Generation gen, final int shift) {
            super(BF);
            this.gen = gen;
            this.shift = shift;
        }

        BranchMap(final Generation gen, final int shift, final Object[] children) {
            super(children);
            this.gen = gen;
            this.shift = shift;
        }

        private BranchMap(final Generation gen, final BranchMap src) {
            super(BF);
            this.gen = gen;
            this.shift = src.shift;
            for (int i = 0; i < BF; ++i) {
                lazySet(i, src.get(i));
            }
        }

        BranchMap<K,V> cloneForWrite(final Generation gen) {
            return new BranchMap<K,V>(gen, this);
        }

        boolean containsKey(final K key, final int hash) {
            final Object child = getChild(hash);
            if (child instanceof LeafMap) {
                return ((LeafMap<K,V>) child).containsKeyU(key, hash);
            } else {
                return ((BranchMap<K,V>) child).containsKey(key, hash);
            }
        }

        private Object getChild(final int hash) {
            final int i = indexFor(hash);
            Object result = get(i);
            if (result == null) {
                // try to create the leaf
                result = new LeafMap<K,V>(gen);
                if (!compareAndSet(i, null, result)) {
                    // someone else succeeded
                    result = get(i);
                }
            }
            return result;
        }

        private int indexFor(final int hash) {
            return (hash >> shift) & BF_MASK;
        }

        /** This is only valid for a quiesced map. */
        boolean containsValueQ(final Object value) {
            for (int i = 0; i < BF; ++i) {
                final Object child = get(i);
                if (child instanceof LeafMap) {
                    if (((LeafMap<K,V>) child).containsValueQ(value)) {
                        return true;
                    }
                } else {
                    if (((BranchMap<K,V>) child).containsValueQ(value)) {
                        return true;
                    }
                }
            }
            return false;
        }

        V get(final K key, final int hash) {
            final Object child = getChild(hash);
            if (child instanceof LeafMap) {
                return ((LeafMap<K,V>) child).getU(key, hash);
            } else {
                return ((BranchMap<K,V>) child).get(key, hash);
            }
        }

        V put(final K key, final int hash, final V value) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.putL(key, hash, value);
                    }
                }
            }
            return unsharedBranch(hash, child).put(key, hash, value);
        }

        private Object prepareForLeafMutationL(final int hash, final LeafMap<K,V> leaf) {
            if (leaf.hasSplitL()) {
                // leaf was split between our getChild and our lock, reread
                return get(indexFor(hash));
            } else if (leaf.shouldSplitL()) {
                // no need to CAS, because everyone is using the lock
                final int newShift = shift + LOG_BF;
                final Object repl = new BranchMap<K,V>(gen, newShift, leaf.splitL(newShift));
                lazySet(indexFor(hash), repl);
                return repl;
            } else if (leaf.gen != gen) {
                // copy-on-write
                final Object repl = leaf.cloneForWriteL(gen);
                lazySet(indexFor(hash), repl);
                return repl;
            } else {
                // OKAY
                return null;
            }
        }

        private BranchMap<K,V> unsharedBranch(final int hash, final Object child) {
            final BranchMap<K,V> branch = (BranchMap<K,V>) child;
            if (branch.gen == gen) {
                return branch;
            } else {
                final BranchMap<K,V> fresh = branch.cloneForWrite(gen);
                final int i = indexFor(hash);
                if (compareAndSet(i, child, fresh)) {
                    return fresh;
                } else {
                    // if we failed someone else succeeded
                    return (BranchMap<K,V>) get(i);
                }
            }
        }

        V remove(final K key, final int hash) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.removeL(key, hash);
                    }
                }
            }
            return unsharedBranch(hash, child).remove(key, hash);
        }

        //////// CAS-like

        V putIfAbsent(final K key, final int hash, final V value) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.putIfAbsentL(key, hash, value);
                    }
                }
            }
            return unsharedBranch(hash, child).putIfAbsent(key, hash, value);
        }

        boolean replace(final K key, final int hash, final V oldValue, final V newValue) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.replaceL(key, hash, oldValue, newValue);
                    }
                }
            }
            return unsharedBranch(hash, child).replace(key, hash, oldValue, newValue);
        }

        V replace(final K key, final int hash, final V value) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.replaceL(key, hash, value);
                    }
                }
            }
            return unsharedBranch(hash, child).replace(key, hash, value);
        }

        boolean remove(final K key, final int hash, final V value) {
            Object child = getChild(hash);
            while (child instanceof LeafMap) {
                final LeafMap<K,V> leaf = (LeafMap<K,V>) child;
                synchronized (leaf) {
                    child = prepareForLeafMutationL(hash, leaf);
                    if (child == null) {
                        // no replacement was provided
                        return leaf.removeL(key, hash, value);
                    }
                }
            }
            return unsharedBranch(hash, child).remove(key, hash, value);
        }
    }

    static class COWMgr<K,V> extends CopyOnWriteManager<BranchMap<K,V>> {
        COWMgr() {
            super(new BranchMap<K,V>(new Generation(), LOG_BF), 0);
        }

        protected BranchMap<K, V> freezeAndClone(final BranchMap<K,V> value) {
            return value.cloneForWrite(new Generation());
        }

        protected BranchMap<K, V> cloneFrozen(final BranchMap<K,V> frozenValue) {
            return frozenValue.cloneForWrite(new Generation());
        }
    }

    private volatile COWMgr<K,V> rootHolder = new COWMgr<K,V>();

    private static int hash(int h) {
        // taken from ConcurrentHashMap
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    //////// construction and cloning

    // TODO: implement clone()

    //////// public interface

    public boolean isEmpty() {
        return rootHolder.isEmpty();
    }

    public int size() {
        return rootHolder.size();
    }

    public boolean containsKey(final Object key) {
        return rootHolder.read().containsKey((K) key, hash(key.hashCode()));
    }

    public boolean containsValue(final Object value) {
        return rootHolder.frozen().containsValueQ(value);
    }

    public V get(final Object key) {
        return rootHolder.read().get((K) key, hash(key.hashCode()));
    }

    public V put(final K key, final int hash, final V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().put(key, hash(key.hashCode()), value);
            if (prev == null) {
                sizeDelta = 1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    public V remove(final Object key) {
        if (key == null) {
            return null;
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().remove((K) key, hash(key.hashCode()));
            if (prev != null) {
                sizeDelta = -1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    //////// CAS-like

    public V putIfAbsent(final K key, final V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().put(key, hash(key.hashCode()), value);
            if (prev == null) {
                sizeDelta = 1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        if (key == null || oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        try {
            return rootHolder.mutable().replace(key, hash(key.hashCode()), oldValue, newValue);
        } finally {
            ticket.leave(0);
        }
    }

    public V replace(final K key, final V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        try {
            return rootHolder.mutable().replace(key, hash(key.hashCode()), value);
        } finally {
            ticket.leave(0);
        }
    }

    public boolean remove(final Object key, final Object value) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (value == null) {
            return false;
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final boolean result = rootHolder.mutable().remove((K) key, hash(key.hashCode()), (V) value);
            if (result) {
                sizeDelta = -1;
            }
            return result;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    public Set<K> keySet() {
        return new KeySet();
    }

    public Collection<V> values() {
        return new Values();
    }

    public Set<Entry<K,V>> entrySet() {
        return new EntrySet();
    }

    //////// Map support classes

    class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return new KeyIterator(rootHolder.frozen());
        }
        public boolean isEmpty() {
            return SnapHashMap.this.isEmpty();
        }
        public int size() {
            return SnapHashMap.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap.this.containsKey(o);
        }
        public boolean remove(Object o) {
            return SnapHashMap.this.remove(o) != null;
        }
        public void clear() {
            SnapHashMap.this.clear();
        }
    }

    final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIterator(rootHolder.frozen());
        }
        public boolean isEmpty() {
            return SnapHashMap.this.isEmpty();
        }
        public int size() {
            return SnapHashMap.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap.this.containsValue(o);
        }
        public void clear() {
            SnapHashMap.this.clear();
        }
    }

    final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        public Iterator<Map.Entry<K,V>> iterator() {
            return new EntryIterator(rootHolder.frozen());
        }
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            V v = SnapHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            return SnapHashMap.this.remove(e.getKey(), e.getValue());
        }
        public boolean isEmpty() {
            return SnapHashMap.this.isEmpty();
        }
        public int size() {
            return SnapHashMap.this.size();
        }
        public void clear() {
            SnapHashMap.this.clear();
        }
    }

    //TODO: implement
    //TODO: implement
    //TODO: implement
    //TODO: implement
    //TODO: implement
    static abstract class AbstractIter<K,V> {
        AbstractIter(final BranchMap<K,V> frozenRoot) {

        }

        public boolean hasNext() {
            return false;
        }

        HashEntry<K,V> nextEntry() {
            return null;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    final static class KeyIterator<K,V> extends AbstractIter<K,V> implements Iterator<K> {
        KeyIterator(final BranchMap<K, V> frozenRoot) {
            super(frozenRoot);
        }

        public K next() {
            return nextEntry().key;
        }
    }

    final static class ValueIterator<K,V> extends AbstractIter<K,V> implements Iterator<V> {
        ValueIterator(final BranchMap<K, V> frozenRoot) {
            super(frozenRoot);
        }

        public V next() {
            return nextEntry().value;
        }
    }

    final static class EntryIterator<K,V> extends AbstractIter<K,V> implements Iterator<Map.Entry<K,V>> {
        EntryIterator(final BranchMap<K, V> frozenRoot) {
            super(frozenRoot);
        }

        public Map.Entry<K,V> next() {
            final HashEntry<K,V> e = nextEntry();
            return new SimpleImmutableEntry<K,V>(e.key, e.value);
        }
    }
}