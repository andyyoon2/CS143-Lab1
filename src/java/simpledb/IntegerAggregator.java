package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int m_gbfield;
    private Type m_gbfieldtype;
    private int m_afield;
    private Op m_what;

    private HashMap<Object, Integer> m_grouping; // Maps the group-by field to its aggregate value

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        m_what = what;
        m_grouping = new HashMap<Object, Integer>();
    }

    private int initial_value() {
        switch(m_what) {
            case MIN: return Integer.MAX_VALUE;
            case MAX: return Integer.MIN_VALUE;
            default: return 0; // SUM, AVG, COUNT
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Object key;
        if (m_gbfield == NO_GROUPING || m_gbfieldtype == null) { key = null; }
        else { Object key = tup.getField(m_gbfield); }

        if (!m_grouping.containsKey(key)) {
            // Haven't encountered this group value yet, create new grouping
            m_grouping.put(key, initial_value());
        }
        int val = m_grouping.get(key);
        int field_val = key.getValue();


    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
