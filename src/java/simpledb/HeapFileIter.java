package simpledb;

import java.util.*;

/**
 * Iterator class for Heap Files
 */
public class HeapFileIter implements DbFileIterator {

    //Need variables for our transactionID and heapfile for constructor
    private TransactionId m_tid;
    private HeapFile m_file;

    //Keep track of our pages, their IDs, and the number of pages we have
    private Page m_page;
    private int m_page_id;
    private int m_num_pages;

    //Need an iterator for our tuples
    private Iterator<Tuple> m_itr;

    //Constructor for our heap file iterator
    public HeapFileIter(TransactionId tid, HeapFile file) {
        m_tid = tid;
        m_file = file;
        //Initially zero
        m_page_id = 0;
        m_num_pages = m_file.numPages();
    }

    // Calls BufferPool.getPage() to access the given page in HeapFile, as mentioned in spec
    private Page read_page(int page_num)
        throws DbException, TransactionAbortedException {

        int pageid = page_num;
        int tableid = m_file.getId();        

        HeapPageId heappid = new HeapPageId(tableid, pageid);
        return Database.getBufferPool().getPage(m_tid, heappid, Permissions.READ_ONLY);
    }

    public void open() throws DbException,TransactionAbortedException {
        m_page = read_page(m_page_id++);
        m_itr = m_page.iterator();
    }

    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if (m_itr == null)
            return false;
        if (m_itr.hasNext())
            return true;
        // Check if there are additional pages
        while (m_page_id < m_num_pages) {
            m_page = read_page(m_page_id++);
            m_itr = m_page.iterator();
            if (m_itr.hasNext())
                return true;
        }
        return false;
    }

    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext())
            return m_itr.next();
        else
            throw new NoSuchElementException("No more tuples in this page");
    }

    public void rewind()
        throws DbException, TransactionAbortedException {
        //Resets the iterator to the start.
        //Same as our TupleIterator
        close();
        open();
    }

    public void close() {
        //Resets out member variables
        m_page_id = 0;
        m_itr = null;
    }
}
