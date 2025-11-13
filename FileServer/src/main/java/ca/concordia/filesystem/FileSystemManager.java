package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
            short head = entries[idx].getFirstBlock();
            freeChain(head);              
            entries[idx] = new FEntry();   
            flushMetadata();
        } finally {
            fsLock.writeLock().unlock();
        }
    }

    public void writeFile(String name, byte[] contents) throws Exception {
        if (contents.length > 0xFFFF) throw new Exception("ERROR: file too large"); 

        fsLock.writeLock().lock(); 
        try {
            int eidx = findEntryIndex(name);
            if (eidx < 0) throw new Exception("ERROR: file " + name + " does not exist");

            int need = (contents.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            List<Integer> free = collectFreeFNodes(need);
            if (free.size() < need) throw new Exception("ERROR: file too large");

            
            int head = -1, prev = -1;
            for (int i = 0; i < need; i++) {
                int fn = free.get(i);
                fnodes[fn].setBlockIndex(fn);  
                fnodes[fn].setNext(-1);
                if (prev >= 0) fnodes[prev].setNext(fn); else head = fn;

                int off = i * BLOCK_SIZE;
                int len = Math.min(BLOCK_SIZE, contents.length - off);
                writeFullBlock(fn, contents, off, len);
                prev = fn;
            }

            short oldHead = entries[eidx].getFirstBlock();
            entries[eidx].setFirstBlock((short) head);
            entries[eidx].setSize((short) contents.length);
            flushMetadata();

            freeChain(oldHead);
            flushMetadata(); 
        } finally {
            fsLock.writeLock().unlock();
        }
    }

    public byte[] readFile(String name) throws Exception {
        fsLock.readLock().lock();
        try {
            int eidx = findEntryIndex(name);
            if (eidx < 0) throw new Exception("ERROR: file " + name + " does not exist");

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
            fsLock.readLock().unlock();
        }
    }

    public String[] listFiles() {
        fsLock.readLock().lock();
        try {
            ArrayList<String> names = new ArrayList<>();
            for (FEntry e : entries) if (e.inUse()) names.add(e.getFilename());
            return names.toArray(new String[0]);
        } finally {
            fsLock.readLock().unlock();
        }
    }

    private void ensureValidName(String n) throws Exception {
        if (n == null || n.isEmpty() || n.length() > 11)
            throw new Exception("ERROR: filename too large");
    }

    private int findEntryIndex(String name) {
        for (int i = 0; i < entries.length; i++)
            if (entries[i].inUse() && entries[i].getFilename().equals(name)) return i;
        return -1;
    }

    private int findFreeEntryIndex() {
        for (int i = 0; i < entries.length; i++)
            if (!entries[i].inUse()) return i;
        return -1;
    }

    private List<Integer> collectFreeFNodes(int need) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < fnodes.length && list.size() < need; i++)
            if (fnodes[i].isFree()) list.add(i);
        return list;
    }

   
    private void freeChain(short head) throws IOException {
        int p = head;
        while (p >= 0) {
            int nxt = fnodes[p].getNext();
            zeroBlock(p);
            fnodes[p].setBlockIndex(-1);
            fnodes[p].setNext(-1);
            p = nxt;
        }
    }

    private int fnodeToDataBlock(int fnodeIdx) { return dataStartBlock + fnodeIdx; }
    private long blockOffset(int blockIndex)   { return (long) blockIndex * BLOCK_SIZE; }

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

    private void readDataBlock(int fnodeIndex, byte[] dst, int off, int len) throws IOException {
        int dataBlock = fnodeToDataBlock(fnodeIndex);
        long pos = blockOffset(dataBlock);
        synchronized (ioLock) {
            disk.seek(pos);
            disk.readFully(dst, off, len);
        }
    }

    private void zeroBlock(int fnodeIndex) throws IOException {
        int dataBlock = fnodeToDataBlock(fnodeIndex);
        long pos = blockOffset(dataBlock);
        synchronized (ioLock) {
            disk.seek(pos);
            disk.write(new byte[BLOCK_SIZE]);
        }
    }

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

    private void loadMetadata() throws IOException {
        synchronized (ioLock) {
            disk.seek(0);
            for (int i = 0; i < MAXFILES; i++) {
                byte[] nameBytes = new byte[12];
                disk.readFully(nameBytes);
                String name = new String(nameBytes, StandardCharsets.US_ASCII);
                int nul = name.indexOf(0);
                if (nul >= 0) name = name.substring(0, nul);
           
                short size = disk.readShort();
                short first = disk.readShort();

                entries[i] = new FEntry();
                entries[i].setFilename(name);
                entries[i].setSize(size);
                entries[i].setFirstBlock(first);
            }
            for (int i = 0; i < MAXBLOCKS; i++) {
                int blockIndex = disk.readInt();
                int next = disk.readInt();
                fnodes[i].setBlockIndex(blockIndex);
                fnodes[i].setNext(next);
            }
        }
    }

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

    private void writeFixedName(String name) throws IOException {
        if (name == null) name = "";
        byte[] buf = new byte[12]; // 11 chars + NUL
        byte[] n = name.getBytes(StandardCharsets.US_ASCII);
        int len = Math.min(11, n.length);
        System.arraycopy(n, 0, buf, 0, len);
        buf[len] = 0;
        synchronized (ioLock) {
            disk.write(buf);
        }
    }
}
