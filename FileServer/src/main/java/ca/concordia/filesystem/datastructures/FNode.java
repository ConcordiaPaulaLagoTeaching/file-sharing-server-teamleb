package ca.concordia.filesystem.datastructures;

public class FNode {
    
    private int blockIndex; 
    private int next;       

    public FNode() { this.blockIndex = -1; this.next = -1; }

    public int getBlockIndex() { return blockIndex; }
    public void setBlockIndex(int blockIndex) { this.blockIndex = blockIndex; }
    public int getNext() { return next; }
    public void setNext(int next) { this.next = next; }

    public boolean isFree() { return blockIndex < 0; }
}
