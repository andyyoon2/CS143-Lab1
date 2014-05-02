package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int m_gbfield;
    private Type m_gbfieldtype;
    private int m_afield;
    private Op m_op;

    private HashMap<Field, Integer> m_grouping; // Maps the group-by field to its aggregate value

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        // Following @throws if != COUNT
        if(!what.equals(Op.COUNT))
            throw new IllegalArgumentException("String Aggregator only supports COUNT, Error!");
        m_op = what;
        m_grouping = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key;
        if (m_gbfield == NO_GROUPING) { key = null; }
        else { key = tup.getField(m_gbfield); }

        if (!m_grouping.containsKey(key)) {
            // Haven't encountered this group value yet, create new grouping
            m_grouping.put(key, 0);
        }
        // Only need to support COUNT, so we will increment every time
        int val = m_grouping.get(key);
        m_grouping.put(key, val+1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        TupleDesc td;
        Type[] types;
        // Create our tuple desc
        if (m_gbfield == NO_GROUPING) {
            types = new Type[1];
            types[0] = Type.INT_TYPE;
        }
        else {
            types = new Type[2];
            types[0] = m_gbfieldtype;
            types[1] = Type.INT_TYPE;
        }
        td = new TupleDesc(types);

        int val;
        Tuple tup;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        // Populate our list with the aggregate values
        for (Field key : m_grouping.keySet()) {
            val = m_grouping.get(key);
            IntField field = new IntField(val);
            tup = new Tuple(td);
            if (m_gbfield == NO_GROUPING) {
                tup.setField(0, field);
            }
            else {
                tup.setField(0, key);
                tup.setField(1, field);
            }
            tuples.add(tup);
        }
        return new TupleIterator(td, tuples);    
    }

}
