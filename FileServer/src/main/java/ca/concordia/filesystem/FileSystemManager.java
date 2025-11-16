package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private final int BLOCK_SIZE = 128;
    private final int FENTRY_SIZE = 15;
    private final int FNODE_SIZE = 4;

    private final int metadataBytes;
    private final int metadataBlocks;

    private final RandomAccessFile disk;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private FEntry[] inodeTable;
    private boolean[] freeBlockList;
    private FNode[] nodeTable;

    public FileSystemManager(String filename, int totalSize) {
        try {
            this.disk = new RandomAccessFile(filename, "rw");
            this.metadataBytes = MAXFILES * FENTRY_SIZE + MAXBLOCKS * FNODE_SIZE;
            this.metadataBlocks = (int) Math.ceil(metadataBytes / (double) BLOCK_SIZE);

            this.inodeTable = new FEntry[MAXFILES];
            this.freeBlockList = new boolean[MAXBLOCKS];
            this.nodeTable = new FNode[MAXBLOCKS];

            long expectedLength = (long) (metadataBlocks + MAXBLOCKS) * BLOCK_SIZE;

            if (disk.length() != expectedLength) {
                // Always reset if file is new or corrupted
                disk.setLength(expectedLength);
                for (int i = 0; i < MAXFILES; i++) inodeTable[i] = null;
                for (int i = 0; i < MAXBLOCKS; i++) {
                    freeBlockList[i] = true;
                    nodeTable[i] = new FNode(-1);
                    nodeTable[i].setNext(-1);
                }
                saveMetadata();
            } else {
                loadMetadata();
            }

            System.out.println("File system initialized: " + filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file system", e);
        }
    }

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

    private long dataBlockOffset(short blockIndex) {
        return (long) (metadataBlocks + blockIndex) * BLOCK_SIZE;
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

    private void freeBlockChain(short firstBlock) throws Exception {
        short current = firstBlock;
        byte[] zeros = new byte[BLOCK_SIZE];

        while (current >= 0) {
            long offset = dataBlockOffset(current);
            disk.seek(offset);
            disk.write(zeros);

            freeBlockList[current] = true;
            int next = nodeTable[current].getNext();
            nodeTable[current].setBlockIndex(-1);
            nodeTable[current].setNext(-1);
            current = (short) next;
        }
    }

    private void saveMetadata() throws IOException {
        for (int i = 0; i < MAXFILES; i++) {
            long pos = (long) i * FENTRY_SIZE;
            disk.seek(pos);

            FEntry e = inodeTable[i];
            if (e == null) {
                for (int b = 0; b < FENTRY_SIZE; b++) disk.writeByte(0);
            } else {
                byte[] nameBytes = e.getFilename().getBytes(StandardCharsets.US_ASCII);
                byte[] name = new byte[11];
                int len = Math.min(11, nameBytes.length);
                System.arraycopy(nameBytes, 0, name, 0, len);
                disk.write(name);
                disk.writeShort(e.getFilesize());
                disk.writeShort(e.getFirstBlock());
            }
        }

        long base = (long) MAXFILES * FENTRY_SIZE;
        for (int i = 0; i < MAXBLOCKS; i++) {
            long pos = base + (long) i * FNODE_SIZE;
            disk.seek(pos);
            disk.writeShort((short) nodeTable[i].getBlockIndex());
            disk.writeShort((short) nodeTable[i].getNext());
        }
    }

    private void loadMetadata() throws IOException {
        byte[] nameBuf = new byte[11];
        for (int i = 0; i < MAXFILES; i++) {
            long pos = (long) i * FENTRY_SIZE;
            disk.seek(pos);
            disk.readFully(nameBuf);
            short size = disk.readShort();
            short firstBlock = disk.readShort();

            if (nameBuf[0] == 0) {
                inodeTable[i] = null;
            } else {
                String name = new String(nameBuf, StandardCharsets.US_ASCII).trim();
                inodeTable[i] = name.isEmpty() ? null : new FEntry(name, size, firstBlock);
            }
        }

        long base = (long) MAXFILES * FENTRY_SIZE;
        for (int i = 0; i < MAXBLOCKS; i++) {
            long pos = base + (long) i * FNODE_SIZE;
            disk.seek(pos);
            short blockIndex = disk.readShort();
            short next = disk.readShort();
            nodeTable[i] = new FNode(blockIndex);
            nodeTable[i].setNext(next);
            freeBlockList[i] = (blockIndex < 0);
        }
    }

    public void createFile(String fileName) throws Exception {
        rwLock.writeLock().lock();
        try {
            if (findFileIndex(fileName) != -1) throw new Exception("File already exists.");
            int inodeIndex = -1;
            for (int i = 0; i < inodeTable.length; i++) {
                if (inodeTable[i] == null) {
                    inodeIndex = i;
                    break;
                }
            }
            if (inodeIndex == -1) throw new Exception("No free inode slots.");
            short blockIndex = allocateBlock();
            inodeTable[inodeIndex] = new FEntry(fileName, (short) 0, blockIndex);
            saveMetadata();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        rwLock.writeLock().lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found.");
            short first = inodeTable[idx].getFirstBlock();
            if (first >= 0) freeBlockChain(first);
            inodeTable[idx] = null;
            saveMetadata();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void writeFile(String fileName, byte[] data) throws Exception {
        short firstBlock = -1;
        short[] allocatedBlocks;
        int blocksNeeded = (int) Math.ceil(data.length / (double) BLOCK_SIZE);

        rwLock.writeLock().lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found.");
            if (countFreeBlocks() < blocksNeeded) throw new Exception("Not enough space.");

            FEntry entry = inodeTable[idx];

            // Free old block chain if it exists
            short oldFirst = entry.getFirstBlock();
            if (oldFirst >= 0) freeBlockChain(oldFirst);

            // Allocate new blocks
            allocatedBlocks = new short[blocksNeeded];
            for (int i = 0; i < blocksNeeded; i++) {
                allocatedBlocks[i] = allocateBlock();
            }

            // Link blocks
            for (int i = 0; i < blocksNeeded - 1; i++) {
                nodeTable[allocatedBlocks[i]].setNext(allocatedBlocks[i + 1]);
            }

            firstBlock = allocatedBlocks[0];
            entry.setFirstBlock(firstBlock);
            entry.setFilesize((short) data.length);
            saveMetadata();
        } finally {
            rwLock.writeLock().unlock();
        }

        // Write data to disk outside the lock
        int offset = 0;
        for (short block : allocatedBlocks) {
            int len = Math.min(BLOCK_SIZE, data.length - offset);
            disk.seek(dataBlockOffset(block));
            disk.write(data, offset, len);
            offset += len;
        }
    }

    public byte[] readFile(String fileName) throws Exception {
        rwLock.readLock().lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found.");
            FEntry entry = inodeTable[idx];
            int size = entry.getFilesize();
            if (size <= 0) return new byte[0];

            byte[] result = new byte[size];
            short current = entry.getFirstBlock();
            int offset = 0;

            while (current >= 0 && offset < size) {
                long diskOffset = dataBlockOffset(current);
                disk.seek(diskOffset);
                int len = Math.min(BLOCK_SIZE, size - offset);
                disk.readFully(result, offset, len);
                offset += len;
                current = (short) nodeTable[current].getNext();
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String[] listFiles() {
        rwLock.readLock().lock();
        try {
            return java.util.Arrays.stream(inodeTable)
                    .filter(e -> e != null)
                    .map(FEntry::getFilename)
                    .toArray(String[]::new);
        } finally {
            rwLock.readLock().unlock();
        }
    }
}