package ca.concordia.filesystem.datastructures;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FEntry {
    // name <= 11 chars; empty string => free entry
    private String filename;
    private short size;         // bytes
    private short firstBlock;   // head fnode index, -1 if empty

    // per-file synchronization: many readers, single writer
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock(true);

    public FEntry() {
        this.filename = "";
        this.size = 0;
        this.firstBlock = -1;
    }

    public FEntry(String filename, int size, int firstBlock) {
        setFilename(filename);
        setSize((short) size);
        setFirstBlock((short) firstBlock);
    }

    public String getFilename() { return filename; }
    public void setFilename(String filename) {
        if (filename == null) filename = "";
        if (filename.length() > 11)
            throw new IllegalArgumentException("Filename cannot be longer than 11 characters.");
        this.filename = filename;
    }

    public short getSize() { return size; }
    public void setSize(short size) {
        if (size < 0) throw new IllegalArgumentException("Filesize cannot be negative.");
        this.size = size;
    }

    public short getFirstBlock() { return firstBlock; }
    public void setFirstBlock(short firstBlock) { this.firstBlock = firstBlock; }

    public boolean inUse() { return filename != null && !filename.isEmpty(); }

    // ---- read/write lock API ----
    public void acquireRead()  { rw.readLock().lock();  }
    public void releaseRead()  { rw.readLock().unlock(); }
    public void acquireWrite() { rw.writeLock().lock(); }
    public void releaseWrite() { rw.writeLock().unlock(); }
}
