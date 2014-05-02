package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    //Our member variables for JoinPredicate
    int m_field1;
    int m_field2;
    Predicate.Op m_op;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        m_field1 = field1;
        m_field2 = field2;
        m_op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here

        Field field1 = t1.getField(m_field1);
        Field field2 = t2.getField(m_field2);

        return field1.compare(m_op,field2);
    }
    
    public int getField1()
    {
        // some code goes here
        return m_field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return m_field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return m_op;
    }
}
