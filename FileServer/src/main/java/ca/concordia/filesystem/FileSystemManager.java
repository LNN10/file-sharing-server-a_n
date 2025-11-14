package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import ca.concordia.filesystem.datastructures.FNode;
import java.io.IOException;


import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    //private final static FileSystemManager instance;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(); // NEW lock for read/write/list

    private static final int BLOCK_SIZE = 128; // Example block size

    // On-disk struct sizes (from assignment example)
    private static final int FENTRY_SIZE = 15; 
    private static final int FNODE_SIZE  = 4;  

    // Computed metadata region size (bytes and blocks)
    private final int metadataBytes;
    private final int metadataBlocks; 

    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks
    private FNode[] nodeTable;  // Array of FNodes

    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        try {
            // Open or create the virtual disk file
            this.disk = new RandomAccessFile(filename, "rw");//rw-> read/write

            // Calculate metadata sizes
            this.metadataBytes = MAXFILES * FENTRY_SIZE + MAXBLOCKS * FNODE_SIZE;
            this.metadataBlocks = (int) Math.ceil(metadataBytes / (double) BLOCK_SIZE);

            // Initialize inode table (metadata for each file)
            this.inodeTable = new FEntry[MAXFILES];

            this.freeBlockList = new boolean[MAXBLOCKS]; // Initialize free block list
            this.nodeTable = new FNode[MAXBLOCKS]; // Initialize FNode table

            // default in-memory state: all blocks free, fnodes unused
            for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true;
                nodeTable[i] = new FNode(-1);  // -1 => free
                nodeTable[i].setNext(-1);
            }

            if (disk.length() == 0) {
                // brand-new disk: size = metadata + data blocks
                long totalBytes = (long) (metadataBlocks + MAXBLOCKS) * BLOCK_SIZE;
                disk.setLength(totalBytes);
                saveMetadata(); // write empty metadata structures
            } else {
                // existing disk: load metadata into memory
                loadMetadata();
            }

            System.out.println("File system initialized: " + filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file system", e);
        }
    }


    // -------More helpers----------
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

        // Convert block index to byte offset on disk
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
        // Free a chain of blocks starting from firstBlock
    private void freeBlockChain(short firstBlock) throws Exception {
        short current = firstBlock;
        byte[] zeros = new byte[BLOCK_SIZE];

        try {
            while (current >= 0) {
                // Overwrite the data block with zeroes
                long offset = dataBlockOffset(current);
                disk.seek(offset);
                disk.write(zeros);

                // Mark free in metadata
                freeBlockList[current] = true;
                int next = nodeTable[current].getNext();
                nodeTable[current].setBlockIndex(-1);
                nodeTable[current].setNext(-1);
                current = (short) next;
            }
        } catch (IOException e) {
            throw new Exception("I/O error while clearing blocks", e);
        }
    }
    // Save inodeTable and nodeTable from memory to disk to remember changes.
    private void saveMetadata() throws IOException {
        // -----FEntries-----
        for (int i = 0; i < MAXFILES; i++) {
            long pos = (long) i * FENTRY_SIZE;
            disk.seek(pos);

            FEntry e = inodeTable[i];
            if (e == null) {
                // write empty 15 bytes
                for (int b = 0; b < FENTRY_SIZE; b++) {
                    disk.writeByte(0);
                }
            } else {
                // filename: max 11 bytes, padded with 0
                byte[] nameBytes = e.getFilename().getBytes(StandardCharsets.US_ASCII);
                byte[] name = new byte[11];
                int len = Math.min(11, nameBytes.length);
                System.arraycopy(nameBytes, 0, name, 0, len);
                disk.write(name);

                disk.writeShort(e.getFilesize());
                disk.writeShort(e.getFirstBlock());
            }
        }

        // -----FNodes-----
        long base = (long) MAXFILES * FENTRY_SIZE;
        for (int i = 0; i < MAXBLOCKS; i++) {
            long pos = base + (long) i * FNODE_SIZE;
            disk.seek(pos);

            int blockIndex = nodeTable[i].getBlockIndex();
            int next = nodeTable[i].getNext();
            disk.writeShort((short) blockIndex);
            disk.writeShort((short) next);
        }
    }

    
    //Load inodeTable and nodeTable from disk into memory.
    
    private void loadMetadata() throws IOException {
        // -----FEntries-----
        byte[] nameBuf = new byte[11];
        for (int i = 0; i < MAXFILES; i++) {
            long pos = (long) i * FENTRY_SIZE;
            disk.seek(pos);

            disk.readFully(nameBuf);
            short size = disk.readShort();
            short firstBlock = disk.readShort();

            // If first character of name is 0, treat as empty entry
            if (nameBuf[0] == 0) {
                inodeTable[i] = null;
            } else {
                String name = new String(nameBuf, StandardCharsets.US_ASCII).trim();
                if (!name.isEmpty()) {
                    try {
                        inodeTable[i] = new FEntry(name, size, firstBlock);
                    } catch (IllegalArgumentException ex) {
                        // In case of corrupted metadata, consider entry empty
                        inodeTable[i] = null;
                    }
                } else {
                    inodeTable[i] = null;
                }
            }
        }

        // -----FNodes-----
        long base = (long) MAXFILES * FENTRY_SIZE;
        for (int i = 0; i < MAXBLOCKS; i++) {
            long pos = base + (long) i * FNODE_SIZE;
            disk.seek(pos);

            short blockIndex = disk.readShort();
            short next = disk.readShort();

            nodeTable[i] = new FNode(blockIndex);
            nodeTable[i].setNext(next);

            // blockIndex < 0 => free, otherwise used
            freeBlockList[i] = (blockIndex < 0);
        }
    }


    // -----API methods for file operations-----


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
            short blockIndex = allocateBlock(); // To avoid code duplication

            //  Initialize node for this block
            nodeTable[blockIndex].setBlockIndex(blockIndex);
            nodeTable[blockIndex].setNext(-1);   // end of chain for now

            //  Create entry
            inodeTable[inodeIndex] = new FEntry(fileName, (short) 0, blockIndex);
            System.out.println("File created: " + fileName + " at block " + blockIndex);

            // save metadata to disk
            saveMetadata();
        } finally {
            globalLock.unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {

        globalLock.lock();
            try {
                //  Locate the file's inode
                int idx = findFileIndex(fileName);
                if (idx == -1) {
                    throw new Exception("File not found.");
                }

                //  Free every block in the chain
                FEntry entry = inodeTable[idx];
                short first = entry.getFirstBlock();

                if (first >= 0) {
                    freeBlockChain(first);
                }

                // Remove the inode
                inodeTable[idx] = null;

                System.out.println("File deleted: " + fileName);

            } finally {
                globalLock.unlock();
            }
        }


        // Write data to file
    public void writeFile(String fileName, byte[] data) throws Exception {
        rwLock.writeLock().lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[idx];

            int blocksNeeded = (int) Math.ceil(data.length / (double) BLOCK_SIZE);
            if (blocksNeeded == 0) {
                // Free old blocks if overwriting
                short oldFirst = entry.getFirstBlock();
                if (oldFirst >= 0) {
                    freeBlockChain(oldFirst);
                }
                entry.setFilesize((short) 0);
                entry.setFirstBlock((short) -1);
                saveMetadata();
                return;
            }

            // Check for available space first
            if (countFreeBlocks() < blocksNeeded) {
                throw new Exception("Not enough space to write file.");
            }
            
            short oldFirst = entry.getFirstBlock();
            if (oldFirst >= 0) {
                freeBlockChain(oldFirst);
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
                long diskOffset = dataBlockOffset(block);
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
            saveMetadata();

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // Read file data
    public byte[] readFile(String fileName) throws Exception {
        rwLock.readLock().lock();
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
            rwLock.readLock().unlock();
        }
    }

    // list files
    public String[] listFiles() {
        rwLock.readLock().lock();
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
            rwLock.readLock().unlock();
        }
    }







}
