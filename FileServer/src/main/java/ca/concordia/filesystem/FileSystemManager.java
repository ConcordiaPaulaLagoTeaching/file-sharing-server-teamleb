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
    public byte[] readFile(String name) throws Exception {    // Reads a file's data from the virtual filesystem
        fsLock.readLock().lock(); // lock the whole filesystem for reading
        try {
            int eidx = findEntryIndex(name);
            if (eidx < 0) throw new Exception("ERROR: file " + name + " does not exist");

            ReentrantReadWriteLock lk = fileLocks.computeIfAbsent(name, k -> new ReentrantReadWriteLock(true));  // each file also has its own lock
            lk.readLock().lock();
            try {
                int size = Short.toUnsignedInt(entries[eidx].getSize());
                byte[] out = new byte[size];
                int fn = entries[eidx].getFirstBlock();
                int pos = 0;
                while (fn >= 0 && pos < size) {
                    int len = Math.min(BLOCK_SIZE, size - pos);
                    readDataBlock(fn, out, pos, len);
                    pos += len;
                    fn = fnodes[fn].getNext();
                }
                return out;
            } finally {
                lk.readLock().unlock();
            }
        } finally {
            fsLock.readLock().unlock();
        }
    }
       // returns a list of all filenames currently in the filesystem
    public String[] listFiles() {
        fsLock.readLock().lock();
        try {
            ArrayList<String> names = new ArrayList<>();
            for (FEntry e : entries) 
                if (e.inUse()) names.add(e.getFilename());
            return names.toArray(new String[0]);
        } finally {
            fsLock.readLock().unlock();
        }
    }

    // checks if the given name is valid (not too long or empty)
    private void ensureValidName(String n) throws Exception {
        if (n == null || n.isEmpty() || n.length() > 11)
            throw new Exception("ERROR: filename too large");
    }

    // searches for a file entry with the given name
    private int findEntryIndex(String name) {
        for (int i = 0; i < entries.length; i++)
            if (entries[i].inUse() && entries[i].getFilename().equals(name)) return i;
        return -1;
    }

    // finds the first empty slot for a new file entry
    private int findFreeEntryIndex() {
        for (int i = 0; i < entries.length; i++)
            if (!entries[i].inUse()) return i;
        return -1;
    }

    // collects a list of free fnodes (unused blocks)
    private List<Integer> collectFreeFNodes(int need) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < fnodes.length && list.size() < need; i++)
            if (fnodes[i].isFree()) list.add(i);
        return list;
    }

    // frees all blocks linked to a file
    private void freeChain(short head) throws IOException {
        int p = head;
        while (p >= 0) {
            int nxt = fnodes[p].getNext();
            zeroBlock(p);
            fnodes[p].setBlockIndex(-1);
            fnodes[p].setNext(-1);
            p = nxt;
        }
        flushMetadata();
    }

    // helper functions to map fnodes to data blocks
    private int fnodeToDataBlock(int fnodeIdx) { return dataStartBlock + fnodeIdx; }
    private long blockOffset(int blockIndex)   { return (long) blockIndex * BLOCK_SIZE; }

    // writes one full block of data to disk
    private void writeFullBlock(int fnodeIndex, byte[] src, int off, int len) throws IOException {
        int dataBlock = fnodeToDataBlock(fnodeIndex);
        long pos = blockOffset(dataBlock);
        synchronized (ioLock) {
            disk.seek(pos);
            if (len == BLOCK_SIZE) {
                disk.write(src, off, len);
            } else {
                byte[] buf = new byte[BLOCK_SIZE];
                System.arraycopy(src, off, buf, 0, len);
                disk.write(buf);
            }
        }
    }

    // reads a block of data from disk
    private void readDataBlock(int fnodeIndex, byte[] dst, int off, int len) throws IOException {
        int dataBlock = fnodeToDataBlock(fnodeIndex);
        long pos = blockOffset(dataBlock);
        synchronized (ioLock) {
            disk.seek(pos);
            disk.readFully(dst, off, len);
        }
    }

    // fills a block with zeros 
    private void zeroBlock(int fnodeIndex) throws IOException {
        int dataBlock = fnodeToDataBlock(fnodeIndex);
        long pos = blockOffset(dataBlock);
        synchronized (ioLock) {
            disk.seek(pos);
            disk.write(new byte[BLOCK_SIZE]);
        }
    }

    // clears the whole filesystem and resets metadata
    private void freshFormat() throws IOException {
        synchronized (ioLock) {
            disk.seek(0);
            byte[] z = new byte[BLOCK_SIZE];
            for (int i = 0; i < totalBlocks; i++) disk.write(z);
        }
        for (int i = 0; i < MAXFILES; i++) entries[i] = new FEntry();
        for (int i = 0; i < MAXBLOCKS; i++) fnodes[i] = new FNode();
        flushMetadata();
    }

    // reads  file entries and fnodes from disk
    private void loadMetadata() throws IOException {
        synchronized (ioLock) {
            disk.seek(0);
            for (int i = 0; i < MAXFILES; i++) {
                byte[] nameBytes = new byte[12];
                disk.readFully(nameBytes);
                String name = new String(nameBytes, StandardCharsets.US_ASCII);
                int nul = name.indexOf(0);
                if (nul >= 0) name = name.substring(0, nul);
                name = name.trim();

                short size = disk.readShort();
                short first = disk.readShort();

                entries[i] = new FEntry();
                entries[i].setFilename(name);
                entries[i].setSize(size);
                entries[i].setFirstBlock(first);

                if (entries[i].inUse())
                    fileLocks.putIfAbsent(entries[i].getFilename(), new ReentrantReadWriteLock(true));
            }
            for (int i = 0; i < MAXBLOCKS; i++) {
                int blockIndex = disk.readInt();
                int next = disk.readInt();
                fnodes[i].setBlockIndex(blockIndex);
                fnodes[i].setNext(next);
            }
        }
    }

    // saves metadata back to disk
    private void flushMetadata() throws IOException {
        synchronized (ioLock) {
            disk.seek(0);
            for (int i = 0; i < MAXFILES; i++) {
                writeFixedName(entries[i].getFilename());
                disk.writeShort(entries[i].getSize());
                disk.writeShort(entries[i].getFirstBlock());
            }
            for (int i = 0; i < MAXBLOCKS; i++) {
                disk.writeInt(fnodes[i].getBlockIndex());
                disk.writeInt(fnodes[i].getNext());
            }
            long wrote = (long) MAXFILES * FENTRY_BYTES + (long) MAXBLOCKS * FNODE_BYTES;
            long pad = (long) metaBlocks * BLOCK_SIZE - wrote;
            if (pad > 0) disk.write(new byte[(int) pad]);
        }
    }

    // writes the filename into a fixed size buffer (used when saving entries)
    private void writeFixedName(String name) throws IOException {
        if (name == null) name = "";
        byte[] buf = new byte[12]; 
        byte[] n = name.getBytes(StandardCharsets.US_ASCII);
        int len = Math.min(11, n.length);
        System.arraycopy(n, 0, buf, 0, len);
        buf[len] = 0;
        synchronized (ioLock) {
            disk.write(buf);
        }
    }
}

