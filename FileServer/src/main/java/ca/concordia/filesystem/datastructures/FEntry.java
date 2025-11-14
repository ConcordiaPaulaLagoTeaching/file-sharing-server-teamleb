package ca.concordia.filesystem.datastructures;

public class FEntry {
    private String filename;   // max 11 chars  empty = unused
    private short size;        // file size in bytes (unsigned short)
    private short firstBlock;  // index into fnodes, -1 if none

    public FEntry() {  // Default constructor, initializes an unused entry
        this.filename = "";
        this.size = 0;
        this.firstBlock = -1;
    }

    public FEntry(String filename, int size, int firstBlock) {   // Constructs a file entry with given parameters
        setFilename(filename);
        setSize((short) size);
        setFirstBlock((short) firstBlock);
    }

    public String getFilename() { return filename; } 

    // Sets the filename (max 11 characters)
    public void setFilename(String filename) {
        if (filename == null) filename = "";
        if (filename.length() > 11)
            throw new IllegalArgumentException("Filename cannot exceed 11 characters.");
        this.filename = filename;
    }

    public short getSize() { return size; }
    public void setSize(short size) {
    this.size = size;   // store raw 16-bit unsigned value
    }


    public short getFirstBlock() { return firstBlock; }
    public void setFirstBlock(short firstBlock) { this.firstBlock = firstBlock; }

    public boolean inUse() { // Returns true if this entry is in use
        return filename != null && !filename.isEmpty(); 
    }
}