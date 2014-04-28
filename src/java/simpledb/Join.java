package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    //Private member variables
    private JoinPredicate m_p;
    private DbIterator m_child1;
    private DbIterator m_child2;
    // Tuples used in our nested loop join 
    private Tuple tuple1 = null;
    private Tuple tuple2 = null;
    // Boolean used to keep track of if we're grabbing data from the other tuple loop
    private boolean outerLoop = true;
    // Bool used to keep track of whether our loop has more tuples or not
    private boolean nestedLoop = true;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        m_p = p;
        m_child1 = child1;
        m_child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return m_p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        // Needs to return a string
        return m_child1.getTupleDesc().getFieldName(m_p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return m_child2.getTupleDesc().getFieldName(m_p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        //Initialize 2 TupleDescs, then call merge function to merge the two
        TupleDesc child1 = m_child1.getTupleDesc();
        TupleDesc child2 = m_child2.getTupleDesc();
        return TupleDesc.merge(child1, child2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        //Following OrderBy/Project operators
        m_child1.open();
        m_child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        m_child1.close();
        m_child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        m_child1.rewind();
        m_child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // If we're in the outerloop during processing, we can set our nested loop
        // to be whether or not our m_child1 has more tuples. Otherwise
        // we continue our inner loop by continueing with m_child2
        if(outerLoop)
            nestedLoop = m_child1.hasNext();
        // Nested loop implementation
        while(nestedLoop){

            //If we're in the outer loop, it means we can set our tuple1 to the next child
            if(outerLoop)
                tuple1 = m_child1.next();

            while(m_child2.hasNext()){
                
                tuple2 = m_child2.next();            
                if(m_p.filter(tuple1, tuple2)){
                    // Since both tuples match predicate, we'll be using the inner loop
                    outerLoop = false;
            
                    // Merging our tuples
                    Tuple mergedTuples = new Tuple(getTupleDesc());
                    // Set iterators to manually look through and set our fields
                    Iterator<Field> iter1 = tuple1.fields();
                    Iterator<Field> iter2 = tuple2.fields();
                    int index = 0;
                    while (iter1.hasNext()) {
                        mergedTuples.setField(index, iter1.next());
                        index++;;
                    }
                    while (iter2.hasNext()) {
                        mergedTuples.setField(index, iter2.next());
                        index++;
                    }
                    return mergedTuples;
                }
            }
            // Resetting our inner child since we're on the next iteration
            m_child2.rewind();
            nestedLoop = m_child1.hasNext();
            outerLoop = true;
            // Done resetting, next loop
        }
        //Otherwise return null
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {m_child1, m_child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        m_child1 = children[0];
        m_child2 = children[1];
    }

}
