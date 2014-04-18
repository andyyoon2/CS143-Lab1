package simpledb;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    //Private variables
    private TupleDesc m_tupleDesc;
    private File m_file;
    private FileChannel m_file_channel;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here

        //Initialize variables
        m_file = f;
        m_tupleDesc = td;

        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            m_file_channel = raf.getChannel();
        } catch (IOException e) {
            System.err.println("Error reading file channel");
            System.exit(1);
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return m_file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return m_file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        //Calculate the page number and an offset
        int page_num = pid.pageNumber();
        int offset = (BufferPool.PAGE_SIZE * page_num);

        try {
            //Allocate for our buffer to be the page size
            ByteBuffer buf = ByteBuffer.allocate(BufferPool.PAGE_SIZE);

            //Check if we've exceeded our max size, error if we have
            if (offset + BufferPool.PAGE_SIZE > m_file_channel.size()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
                }

            //Otherwise read from the buffer based on the offset
            m_file_channel.read(buf,offset);
            HeapPageId page = (HeapPageId) pid;
            HeapPage result = new HeapPage(page,buf.array());
            return result;

        } catch (IOException e) {
            throw new IllegalArgumentException("Page does not exist, error!");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long pageLen = m_file.length();
        int bufPage = BufferPool.PAGE_SIZE;
        return (int) Math.ceil(pageLen/bufPage);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIter(tid, this);
    }

}

