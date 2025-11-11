package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES;
    private final int MAXBLOCKS;
    private final int BLOCK_SIZE;

    private static final int FENTRY_BYTES = 16; 
    private static final int FNODE_BYTES  = 8;  

    private final RandomAccessFile disk;
    private final Object ioLock = new Object();          
    private final int totalBlocks;
    private final int metaBytes;
    private final int metaBlocks;
    private final int dataStartBlock;

    private final FEntry[] entries;
    private final FNode[]  fnodes;


    private final ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>();

    public FileSystemManager(String backingFilename,
                             int blockSize,
                             int maxFiles,
                             int maxBlocks,
                             int totalSizeBytes) {
        try {
            if (blockSize <= 0 || maxFiles <= 0 || maxBlocks <= 0)
                throw new IllegalArgumentException("All size parameters must be positive.");
            if (totalSizeBytes % blockSize != 0)
                throw new IllegalArgumentException("Total size must be a multiple of BLOCK_SIZE");

            this.BLOCK_SIZE = blockSize;
            this.MAXFILES   = maxFiles;
            this.MAXBLOCKS  = maxBlocks;

            this.totalBlocks = totalSizeBytes / blockSize;

            this.entries = new FEntry[MAXFILES];
            this.fnodes  = new FNode[MAXBLOCKS];
            for (int i = 0; i < MAXFILES; i++) entries[i] = new FEntry();
            for (int i = 0; i < MAXBLOCKS; i++) fnodes[i] = new FNode();

            this.metaBytes      = MAXFILES * FENTRY_BYTES + MAXBLOCKS * FNODE_BYTES;
            this.metaBlocks     = (metaBytes + blockSize - 1) / blockSize;
            this.dataStartBlock = metaBlocks;

            if (dataStartBlock + MAXBLOCKS > totalBlocks)
                throw new IllegalStateException("Not enough space for data blocks (increase totalSizeBytes).");

            File f = new File(backingFilename);
            boolean existed = f.exists();

            this.disk = new RandomAccessFile(f, "rw");
            synchronized (ioLock) {
                this.disk.setLength((long) totalBlocks * blockSize);
            }

            if (!existed) {
                freshFormat();
            } else {
                try { loadMetadata(); }
                catch (Exception corrupt) { freshFormat(); }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createFile(String name) throws Exception {
        fsLock.writeLock().lock();
        try {
            ensureValidName(name);
            if (findEntryIndex(name) >= 0) throw new Exception("ERROR: file already exists");
            int slot = findFreeEntryIndex();
            if (slot < 0) throw new Exception("ERROR: no free file entries");

            entries[slot] = new FEntry(name, 0, -1);
            fileLocks.putIfAbsent(name, new ReentrantReadWriteLock(true));
            flushMetadata();
        } finally {
            fsLock.writeLock().unlock();
        }
    }

    public void deleteFile(String name) throws Exception {
        fsLock.writeLock().lock();
        try {
            int idx = findEntryIndex(name);
            if (idx < 0) throw new Exception("ERROR: file " + name + " does not exist");

            ReentrantReadWriteLock lk = fileLocks.computeIfAbsent(name, k -> new ReentrantReadWriteLock(true));
            lk.writeLock().lock();
            try {
                freeChain(entries[idx].getFirstBlock());
                entries[idx] = new FEntry(); 
                flushMetadata();
            } finally {
                lk.writeLock().unlock();
            }
        } finally {
            fsLock.writeLock().unlock();
        }
    }
  // check that file name is not empty or too long
    private void ensureValidName(String n) throws Exception {
        if (n == null || n.isEmpty() || n.length() > 11) {
            throw new Exception("ERROR: invalid filename");
        }
    }

    // find file entry by name
    private int findEntryIndex(String name) {
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].inUse() && entries[i].getFilename().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    // find the first free entry slot
    private int findFreeEntryIndex() {
        for (int i = 0; i < entries.length; i++) {
            if (!entries[i].inUse()) {
                return i;
            }
        }
        return -1;
    }

    // get a list of free fnode indexes
    private List<Integer> collectFreeFNodes(int need) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < fnodes.length && list.size() < need; i++) {
            if (fnodes[i].isFree()) {
                list.add(i);
            }
        }
        return list;
    }
    // TEMPORARY: stubs for unimplemented functions so code can compile
private void freshFormat() {
    System.out.println("freshFormat() not implemented yet");
}

private void loadMetadata() {
    System.out.println("loadMetadata() not implemented yet");
}

private void flushMetadata() {
    System.out.println("flushMetadata() not implemented yet");
}

private void freeChain(short blockIndex) {
    System.out.println("freeChain() not implemented yet");
}// end of TEMPORARY stubs


}
