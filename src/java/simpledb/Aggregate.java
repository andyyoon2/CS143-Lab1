package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator m_child;
    private int m_afield;
    private int m_gfield;
    private Aggregator.Op m_op;

    private Aggregator m_agg;   // Which aggregator class we're using
    private DbIterator m_it;    // Output aggregator's iterator
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        m_child = child;
        m_afield = afield;
        m_gfield = gfield;
        m_op = aop;
        Type gtype;
        if (gfield == -1)
            gtype = null;
        else
            gtype = child.getTupleDesc().getFieldType(gfield);
        Type atype = child.getTupleDesc().getFieldType(afield);

        if (atype == Type.INT_TYPE) {
            m_agg = new IntegerAggregator(gfield, gtype, afield, aop);
        }
        else if (atype == Type.STRING_TYPE) {
            m_agg = new StringAggregator(gfield, gtype, afield, aop);
        }
        else {
            throw new IllegalArgumentException("Aggregates supported only on Integer and String fields");
        }
        m_it = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return m_gfield;
        // NO_GROUPING is handled by the caller
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (m_gfield == Aggregator.NO_GROUPING)
            return null;
        else
	        return m_it.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return m_afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    if (m_gfield == Aggregator.NO_GROUPING)
	        return m_it.getTupleDesc().getFieldName(0);
        else
	        return m_it.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return m_op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        m_child.open();
        super.open();
        
        // Merge tuples into the groupings
        Tuple tup;
        while(m_child.hasNext()) {
            tup = m_child.next();
            m_agg.mergeTupleIntoGroup(tup);
        }
        // Initialize output iterator
        m_it = m_agg.iterator();
        m_it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if (m_it.hasNext()) {
            return m_it.next();
        }
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        m_it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return m_agg.iterator().getTupleDesc();
    }

    public void close() {
        super.close();
        m_child.close();
        m_it.close();
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
