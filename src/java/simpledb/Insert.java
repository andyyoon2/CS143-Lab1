package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_tid;
    private DbIterator m_child;
    private int m_tableid;
    private boolean m_open;
    private TupleDesc m_td;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        if (!file.getTupleDesc().equals(child.getTupleDesc())) {
            throw new DbException("Insert error: tupledesc mismatch");
        }
        m_tid = t;
        m_child = child;
        m_tableid = tableid;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!m_open) { return null; }
        // Prevent calling this function more than once
        m_open = false;

        // Insert records into the table
        int count = 0;
        while (m_child.hasNext()) {
            Tuple t = m_child.next();
            try {
                Database.getBufferPool().insertTuple(m_tid, m_tableid, t);
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
