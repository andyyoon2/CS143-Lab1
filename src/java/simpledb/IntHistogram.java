package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int m_buckets;
    private int m_min;
    private int m_max;
    private int m_width;
    private int m_tuples;
    private int[] m_histogram;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        m_buckets = buckets;
        m_min = min;
        m_max = max;
        m_tuples = 0;
        m_width = (max-min+1) / buckets;
        if (m_width == 0) { // more buckets than needed
            m_width = 1;
        }

        // Initialize main array
        // Last bucket holds overflow values
        m_histogram = new int[buckets];
        for (int i = 0; i < buckets; i++) {
            m_histogram[i] = 0;
        }
        //System.out.format("\nNew histogram with %d buckets, width %d\n", m_buckets, m_width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > m_max || v < m_min) { //do nothing
            return;
        }
        //System.out.format("adding value %d ",v);
        int bucket_num = (v-m_min) / m_width;
        if (bucket_num < m_buckets) {// Normal
            //System.out.format("to bucketnum %d\n",bucket_num);
            m_histogram[bucket_num] += 1;
        } else {// Overflow, just add to last bucket
            //System.out.format("to bucketnum %d\n",m_buckets-1);
            m_histogram[m_buckets-1] += 1;
        }
        //System.out.format("curr_count:%d\n",m_histogram[bucket_num]);
        m_tuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucket_num, height = 0;
        double contribution = 0;
        bucket_num = (v-m_min) / m_width;
        if (0 < bucket_num && bucket_num < m_buckets) {
            height = m_histogram[bucket_num];
        }
        System.out.format("bucket_num:%d " + "height:%d ",bucket_num,height);

        if (op == Predicate.Op.EQUALS) {
            contribution = (double) height / m_width;
        } else if (op == Predicate.Op.NOT_EQUALS) {
            contribution = (double) m_tuples - height;
        } else {
            // if op == GREATER_THAN or GREATER_THAN_OR_EQ
            //System.out.format("greater than %d: ",v);
            if (v < m_min) {
                contribution = m_tuples;
            } else if (v > m_max) {
                contribution = 0;
            } else {
                double frac = (double) height;
                double right = (bucket_num+1)*m_width + m_min - 1;
                System.out.format("frac is %f. ", frac);
                System.out.format("right endpoint is %f. ", right);
                double part;
                if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
                    part = (right - v + 1) / m_width;
                    System.out.format("greater than or equal to %d:\n",v);
                    System.out.format("part is %f.\n",part);
                } else {
                    part = (right - v) / m_width;
                }
                contribution = frac * part;
                System.out.format("contribution so far: %f\n",contribution);
                for (int i = bucket_num+1; i < m_buckets; i++) {
                    height = m_histogram[i];
                    contribution += height;
                }
            }
            if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
                // take complement of above
                //System.out.format("jk less than: ");
                contribution = m_tuples - contribution;
            }
        }
        System.out.format("%f\n",contribution/m_tuples);
        return contribution / m_tuples;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        /* CODE NOT NEEDED
        int num = 0;
        for (int i : m_histogram.keySet()) {
            int height = m_histogram.get(i);
            num += (height * height);
        }
        return num / (m_tuples * m_tuples);
        */
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "Has " + m_buckets + " buckets, from " + m_min + " to " + m_max + ".\n";
    }
}
