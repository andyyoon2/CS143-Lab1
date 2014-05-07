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
        return m_file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return m_tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
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
        int pnum = page.getId().pageNumber();
        try {
            FileOutputStream fostream = new FileOutputStream(m_file);
            fostream.write(page.getPageData(), BufferPool.PAGE_SIZE * pnum, BufferPool.PAGE_SIZE);
            fostream.close();
        }
        catch (FileNotFoundException e) { System.out.println("File not found"); }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long pageLen = m_file.length();
        int bufPage = BufferPool.PAGE_SIZE;
        return (int) Math.ceil(pageLen/bufPage);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modified = new ArrayList<Page>();
        PageId pid;
        HeapPage page;
        int pnum;
        // Look for an available page
        for (pnum = 0; pnum < numPages(); pnum++) {
            pid = new HeapPageId(getId(), pnum);
            page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                modified.add(page);
                return modified;
            }
        }
        
        // All pages are full
        // Append empty page to end of file
        try {
            FileOutputStream fostream = new FileOutputStream(m_file, true);
            fostream.write(HeapPage.createEmptyPageData());
            fostream.close();
        }
        catch (FileNotFoundException e) { System.out.println("File not found"); }

        // Retreive page
        pid = new HeapPageId(getId(), pnum);
        page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        // Insert tuple
        page.insertTuple(t);
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> modified = new ArrayList<Page>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        page.deleteTuple(t);
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIter(tid, this);
    }

}

