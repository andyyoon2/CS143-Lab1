package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_tid;
    private DbIterator m_child;
    private boolean m_open;
    private TupleDesc m_td;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        m_tid = t;
        m_child = child;
        m_open = false;

        // Create the output tupledesc
        Type[] type = new Type[] { Type.INT_TYPE };
        String[] name = new String[] { "Number of tuples inserted" };
        m_td = new TupleDesc(type, name);
    }

    public TupleDesc getTupleDesc() {
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        m_child.open();
        super.open();
        m_open = true;
    }

    public void close() {
        super.close();
        m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        m_child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!m_open) { return null; }
        // Prevent calling this function more than once
        m_open = false;

        int count = 0;
        while (m_child.hasNext()) {
            Tuple t = m_child.next();
            try {
                Database.getBufferPool().deleteTuple(m_tid, t);
            }
            catch (IOException e) { e.printStackTrace(); }
            count++;
        }

        // Create and return the output tuple
        Tuple out = new Tuple(m_td);
        out.setField(0, new IntField(count));
        return out;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (m_child != children[0]) { m_child = children[0]; }
    }

}
