package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import ca.concordia.filesystem.datastructures.FNode;
import java.io.IOException;


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
    private FNode[] nodeTable;

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

            // Initialize FNode table
            this.nodeTable = new FNode[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                nodeTable[i] = new FNode(-1); // free block metadata
        }

            System.out.println("File system initialized: " + filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file system", e);
        }
    }

    // More helpers
    private int findFileIndex(String fileName) {
        for (int i = 0; i < inodeTable.length; i++) {
            FEntry entry = inodeTable[i];
            if (entry != null && entry.getFilename().equals(fileName)) {
                return i;
            }
        }
        return -1;
    }

    private int countFreeBlocks() {
        int count = 0;
        for (boolean free : freeBlockList) {
            if (free) count++;
        }
        return count;
    }

    private short allocateBlock() throws Exception {
        for (short i = 0; i < freeBlockList.length; i++) {
            if (freeBlockList[i]) {
                freeBlockList[i] = false;
                nodeTable[i].setBlockIndex(i);
                nodeTable[i].setNext(-1);
                return i;
            }
        }
        throw new Exception("No free blocks available.");
    }

    private void freeBlockChain(short firstBlock) {
        short current = firstBlock;
        while (current >= 0) {
            freeBlockList[current] = true;
            int next = nodeTable[current].getNext();
            nodeTable[current].setBlockIndex(-1);
            nodeTable[current].setNext(-1);
            current = (short) next;
        }
    }



    public void createFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            //  Check for existing file
            for (FEntry entry : inodeTable) {
                if (entry != null && entry.getFilename().equals(fileName)) {
                    throw new Exception("File already exists.");
                }
            }

            //  Find free inode slot
            int inodeIndex = -1;
            for (int i = 0; i < inodeTable.length; i++) {
                if (inodeTable[i] == null) {
                    inodeIndex = i;
                    break;
                }
            }
            if (inodeIndex == -1) throw new Exception("No free inode slots.");

            //  Find free data block
            short blockIndex = -1;
            for (short i = 0; i < freeBlockList.length; i++) {
                if (freeBlockList[i]) {
                    blockIndex = i;
                    freeBlockList[i] = false;
                    break;
                }
            }
            if (blockIndex == -1) throw new Exception("No free blocks available.");

            //  Initialize node for this block
            nodeTable[blockIndex].setBlockIndex(blockIndex);
            nodeTable[blockIndex].setNext(-1);   // end of chain for now

            //  Create entry
            inodeTable[inodeIndex] = new FEntry(fileName, (short) 0, blockIndex);
            System.out.println("File created: " + fileName + " at block " + blockIndex);

        } finally {
            globalLock.unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int inodeIndex = -1;

            //  Find the file
            for (int i = 0; i < inodeTable.length; i++) {
                FEntry entry = inodeTable[i];
                if (entry != null && entry.getFilename().equals(fileName)) {
                    inodeIndex = i;
                    break;
                }
            }

            if (inodeIndex == -1) {
                throw new Exception("File not found.");
            }

            //  Free the data block
            short block = inodeTable[inodeIndex].getFirstBlock();
            freeBlockList[block] = true;

            //  Remove the inode entry
            inodeTable[inodeIndex] = null;

            System.out.println("File deleted: " + fileName);

        } finally {
            globalLock.unlock();
        }
    }

    // Write data to file
    public void writeFile(String fileName, byte[] data) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[idx];

            // 1) Free old blocks if overwriting
            short oldFirst = entry.getFirstBlock();
            if (oldFirst >= 0) {
                freeBlockChain(oldFirst);
            }

            int blocksNeeded = (int) Math.ceil(data.length / (double) BLOCK_SIZE);
            if (blocksNeeded == 0) {
                entry.setFilesize((short) 0);
                entry.setFirstBlock((short) -1);
                return;
            }

            if (countFreeBlocks() < blocksNeeded) {
                throw new Exception("Not enough space to write file.");
            }

            short firstBlock = -1;
            short prevBlock = -1;
            int dataOffset = 0;

            for (int b = 0; b < blocksNeeded; b++) {
                short block = allocateBlock();

                if (firstBlock == -1) {
                    firstBlock = block;
                } else {
                    nodeTable[prevBlock].setNext(block);
                }
                prevBlock = block;

                int bytesToWrite = Math.min(BLOCK_SIZE, data.length - dataOffset);
                long diskOffset = (long) block * BLOCK_SIZE; // for now, disk used only for data
                try {
                    disk.seek(diskOffset);
                    disk.write(data, dataOffset, bytesToWrite);
                } catch (IOException e) {
                    throw new Exception("I/O error while writing file", e);
                }
                dataOffset += bytesToWrite;
            }

            entry.setFirstBlock(firstBlock);
            entry.setFilesize((short) data.length);
            System.out.println("Wrote " + data.length + " bytes to " + fileName);

        } finally {
            globalLock.unlock();
        }
    }

    // Read file data
    public byte[] readFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[idx];

            int size = entry.getFilesize();
            if (size <= 0) {
                return new byte[0];
            }

            byte[] result = new byte[size];
            short current = entry.getFirstBlock();
            int offsetInResult = 0;

            try {
                while (current >= 0 && offsetInResult < size) {
                    long diskOffset = (long) current * BLOCK_SIZE;
                    disk.seek(diskOffset);

                    int bytesToRead = Math.min(BLOCK_SIZE, size - offsetInResult);
                    disk.readFully(result, offsetInResult, bytesToRead);
                    offsetInResult += bytesToRead;

                    current = (short) nodeTable[current].getNext();
                }
            } catch (IOException e) {
                throw new Exception("I/O error while reading file", e);
            }

            System.out.println("Read " + size + " bytes from " + fileName);
            return result;

        } finally {
            globalLock.unlock();
        }
    }

    // list files
    public String[] listFiles() {
        globalLock.lock();
        try {
            int count = 0;
            for (FEntry entry : inodeTable) {
                if (entry != null) {
                    count++;
                }
            }

            String[] names = new String[count];
            int idx = 0;
            for (FEntry entry : inodeTable) {
                if (entry != null) {
                    names[idx++] = entry.getFilename();
                }
            }

            return names;
        } finally {
            globalLock.unlock();
        }
    }




    // TODO: Add readFile, writeFile and other required methods,


}
