package ca.concordia;

import ca.concordia.server.FileServer;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello and welcome!");

        int blockSize = 128;
        int maxFiles  = 5000;
        int maxBlocks = 2000;

      
        int metaBytes   = maxFiles * 16 + maxBlocks * 8;
        int metaBlocks  = (metaBytes + blockSize - 1) / blockSize;
        int totalBlocks = metaBlocks + maxBlocks + 16;
        int totalSize   = totalBlocks * blockSize;

        FileServer server = new FileServer(
                12345,
                "filesystem.dat",
                blockSize, maxFiles, maxBlocks, totalSize
        );
        server.start();
    }
}
