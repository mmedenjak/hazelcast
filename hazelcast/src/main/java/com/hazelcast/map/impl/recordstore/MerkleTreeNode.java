package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.Collection;
import java.util.Map.Entry;

public class MerkleTreeNode<R extends Record> {
    private static final Object NULL_OBJECT = new Object();
    public int digest;
    private final int maxDepth;
    private final int minHash;
    private final int maxHash;
    private final StorageSCHM<R> storage;
    private MerkleTreeNode left;
    private MerkleTreeNode right;
    private Int2ObjectHashMap<Object> hashes;
    private boolean dirty = false;

    public MerkleTreeNode(int maxDepth, int minHash, int maxHash, StorageSCHM<R> storage) {
        this.maxDepth = maxDepth;
        this.minHash = minHash;
        this.maxHash = maxHash;
        this.storage = storage;
    }

    public void markDirty(int keyHash) {
        if (maxDepth > 0) {
            final int middle = (int) (((long) minHash + maxHash) / 2);
            MerkleTreeNode branch;
            if (keyHash >= minHash && keyHash < middle) {
                if (left == null) {
                    left = new MerkleTreeNode<R>(maxDepth - 1, minHash, middle, storage);
                }
                branch = left;
            } else {
                if (right == null) {
                    right = new MerkleTreeNode<R>(maxDepth - 1, middle, maxHash, storage);
                }
                branch = right;
            }
            branch.markDirty(keyHash);
        } else {
            if (hashes == null) {
                hashes = new Int2ObjectHashMap<Object>();
            }
            hashes.put(keyHash, NULL_OBJECT);
            dirty = true;
        }
    }


    public void update(int keyHash) {
        if (maxDepth > 0) {
            final int middle = (int) (((long) minHash + maxHash) / 2);
            MerkleTreeNode branch;
            if (keyHash >= minHash && keyHash < middle) {
                if (left == null) {
                    left = new MerkleTreeNode<R>(maxDepth - 1, minHash, middle, storage);
                }
                branch = left;
            } else {
                if (right == null) {
                    right = new MerkleTreeNode<R>(maxDepth - 1, middle, maxHash, storage);
                }
                branch = right;
            }
            branch.update(keyHash);
            this.digest = getHash(left != null ? left.digest : 0, right != null ? right.digest : 0);
        } else {
            if (hashes == null) {
                hashes = new Int2ObjectHashMap<Object>();
            }
            hashes.put(keyHash, (Integer) getHashFor(keyHash));
            this.digest = getHash(hashes.values());
        }
    }

    private int getHashFor(int hash) {
        return storage.getHashFor(hash);
    }

    private int getHash(int a, int b) {
        return 31 * a + b;
    }

    private int getHash(Collection<Object> values) {
        int result = 1;
        for (Object value : values) {
            result = 31 * result + (Integer) value;
        }
        return result;
    }

    public void clear() {
        left = right = null;
        hashes = null;
        digest = 0;
    }

    public int[] getRangeSubHashes(int rangeMinKeyHash, int rangeMaxKeyHash) {
        final int middle = (int) (((long) minHash + maxHash) / 2);

        if (rangeMinKeyHash == minHash && rangeMaxKeyHash == maxHash) {
            return new int[]{left != null ? left.digest : 0, right != null ? right.digest : 0};
        } else if (rangeMinKeyHash >= minHash && rangeMinKeyHash <= middle
                && rangeMaxKeyHash >= minHash && rangeMaxKeyHash <= middle) {
            return left != null ? left.getRangeSubHashes(rangeMinKeyHash, rangeMaxKeyHash) : new int[]{0, 0};
        } else if (rangeMinKeyHash >= middle && rangeMinKeyHash <= maxHash
                && rangeMaxKeyHash >= middle && rangeMaxKeyHash <= maxHash) {
            return right != null ? right.getRangeSubHashes(rangeMinKeyHash, rangeMaxKeyHash) : new int[]{0, 0};
        } else {
            throw new RuntimeException("Could not find range");
        }
    }

    public void clean() {
        if (maxDepth > 0) {
            if (left != null) {
                left.clean();
            }
            if (right != null) {
                right.clean();
            }
            this.digest = getHash(left != null ? left.digest : 0, right != null ? right.digest : 0);
        } else if (dirty) {
            //System.out.println("CLEANING MERKLE TREE!");
            if (hashes == null) {
                hashes = new Int2ObjectHashMap<Object>();
            }
            for (Entry<Integer, Object> hashEntry : hashes.entrySet()) {
                if (hashEntry.getValue() == NULL_OBJECT) {
                    hashEntry.setValue(getHashFor(hashEntry.getKey()));
                }
            }
            this.digest = getHash(hashes.values());
            dirty = false;
        }
    }
}
