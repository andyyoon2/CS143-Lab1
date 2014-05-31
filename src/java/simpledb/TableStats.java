package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    private HeapFile m_file;    // The DbFile we're scanning
    private int m_iocostperpage;// IO cost per page
    private int m_tuples;       // Number of tuples in the file

    // Map the field num to their respective min/max values
    private HashMap<Integer, Integer> m_mins;
    private HashMap<Integer, Integer> m_maxs;
    // Map the field num to their histograms
    private HashMap<Integer, IntHistogram> m_inthists;
    private HashMap<Integer, StringHistogram> m_strhists;

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        m_file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        m_iocostperpage = ioCostPerPage;
        m_tuples = 0;
        m_mins = new HashMap<Integer, Integer>();
        m_maxs = new HashMap<Integer, Integer>();
        m_inthists = new HashMap<Integer, IntHistogram>();
        m_strhists = new HashMap<Integer, StringHistogram>();
        
        TupleDesc td = m_file.getTupleDesc();
        DbFileIterator it = m_file.iterator(null);    // Arbitrary transaction id?
        try {
            it.open();
            // Scan table for min/max values
            while(it.hasNext()) {
                Tuple t = it.next();
                m_tuples++;
                for (int i = 0; i < td.numFields(); i++) {
                    // Only concerned about int fields for min/max
                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        int val = ((IntField) t.getField(i)).getValue();
                        if (!m_mins.containsKey(i)) {
                            // Initial value
                            m_mins.put(i, val);
                        } else {
                            if (val < m_mins.get(i)) {
                                // Update with new min value
                                m_mins.put(i, val);
                            }
                        }

                        if (!m_maxs.containsKey(i)) {
                            m_maxs.put(i, val);
                        } else {
                            if (val > m_maxs.get(i)) {
                                m_maxs.put(i, val);
                            }
                        }
                    }
                }// end for
            }// end while
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        // Initialize histograms
        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                int min = m_mins.get(i);
                int max = m_maxs.get(i);
                IntHistogram h = new IntHistogram(NUM_HIST_BINS, min, max);
                m_inthists.put(i, h);
            } else {// STRING_TYPE
                StringHistogram h = new StringHistogram(NUM_HIST_BINS);
                m_strhists.put(i, h);
            }
        }

        try {
            it.rewind();
            // Scan table again to populate our histograms
            while(it.hasNext()) {
                Tuple t = it.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        int val = ((IntField) t.getField(i)).getValue();
                        IntHistogram h = m_inthists.get(i);
                        h.addValue(val);
                        m_inthists.put(i, h);
                    } else {// STRING_TYPE
                        String val = ((StringField) t.getField(i)).getValue();
                        StringHistogram h = m_strhists.get(i);
                        h.addValue(val);
                        m_strhists.put(i, h);
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        int num_pages = m_file.numPages();
        return num_pages * m_iocostperpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(m_tuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType().equals(Type.INT_TYPE)) {
            int val = ((IntField) constant).getValue();
            IntHistogram h = m_inthists.get(field);
            return h.estimateSelectivity(op, val);
        } else {// STRING_TYPE
            String val = ((StringField) constant).getValue();
            StringHistogram h = m_strhists.get(field);
            return h.estimateSelectivity(op, val);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return m_tuples;
    }

}
