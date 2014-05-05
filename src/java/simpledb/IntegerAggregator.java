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
    private Op m_op;

    private HashMap<Field, Integer> m_grouping; // Maps the group-by field to its aggregate value
    private HashMap<Field, Integer> m_average;  // Keeps track of the number of ints to average
    private String m_gname;                     // Name of the field we're grouping by
    private String m_aname;                     // Name of the aggregate field
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
        m_op = what;
        m_grouping = new HashMap<Field, Integer>();
        m_average  = new HashMap<Field, Integer>();
        m_gname = "";
        m_aname = "";
    }

    /** 
     * Returns the initial aggregate value for a new grouping
     */
    private int initial_value() {
        switch(m_op) {
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
        // Initialize key,
        //  field names for output tuple desc
        Field key;
        if (m_gbfield == NO_GROUPING) { key = null; }
        else { 
            key = tup.getField(m_gbfield); 
            m_gname = tup.getTupleDesc().getFieldName(m_gbfield);
        }
        m_aname = "Aggregate " + m_op.toString() + "(" + tup.getTupleDesc().getFieldName(m_afield) + ")";

        if (!m_grouping.containsKey(key)) {
            // Haven't encountered this group value yet, create new grouping
            m_grouping.put(key, initial_value());
        }
        int val = m_grouping.get(key);
        int field_val = ((IntField) tup.getField(m_afield)).getValue();

        // Update aggregate value according to operator
        switch(m_op) {
            case MIN:
                if (field_val < val) { val = field_val; }
                break;
            case MAX:
                if (field_val > val) { val = field_val; }
                break;
            case AVG:
                // Increment the value in the m_average hashmap,
                // division will be taken care of in iterator()
                if (!m_average.containsKey(key)) {
                    m_average.put(key, 1);
                }
                else {
                    int num_values = m_average.get(key);
                    m_average.put(key, num_values+1);
                }
            case SUM:
                val += field_val;
                break;
            case COUNT:
                val++;
                break;
        }
        m_grouping.put(key, val);
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
        Type[] types;
        String[] names;
        // Create our tuple desc
        if (m_gbfield == NO_GROUPING) {
            types = new Type[1];
            types[0] = Type.INT_TYPE;
            names = new String[1];
            names[0] = m_aname;
        }
        else {
            types = new Type[2];
            types[0] = m_gbfieldtype;
            types[1] = Type.INT_TYPE;
            names = new String[2];
            names[0] = m_gname;
            names[1] = m_aname;
        }
        TupleDesc td = new TupleDesc(types,names);

        int val;
        Tuple tup;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        // Populate our list with the aggregate values
        for (Field key : m_grouping.keySet()) {
            val = m_grouping.get(key);
            // AVG division required
            if (m_op == Op.AVG) {
                val /= m_average.get(key);
            }
            
            // Create the tuple
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
