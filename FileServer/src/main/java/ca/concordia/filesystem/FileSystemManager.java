package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private final static FileSystemManager instance;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size

    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks

    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        try {
            // Open or create the virtual disk file
            this.disk = new RandomAccessFile(filename, "rw");//rw-> read/write

            // Initialize inode table (metadata for each file)
            this.inodeTable = new FEntry[MAXFILES];

            // Initialize free block list (bitmap)
            this.freeBlockList = new boolean[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true; // all blocks free at start
            }

            System.out.println("File system initialized: " + filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file system", e);
        }
    }

    public void createFile(String fileName) throws Exception {
        // TODO
        throw new UnsupportedOperationException("Method not implemented yet.");
    }


    // TODO: Add readFile, writeFile and other required methods,
}
