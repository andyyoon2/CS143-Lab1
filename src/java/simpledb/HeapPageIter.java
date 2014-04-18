package simpledb;

import java.util.*;

public class HeapPageIter implements Iterator<Tuple> {

    private HeapPage heapPage;

    //Number of Tuples that are valid, ie empty and not used already
    private int numTuples;

    //Keeps track of our current Tuple
    private int currTuple;
        
    //
    public HeapPageIter(HeapPage page) {
        heapPage = page;
        currTuple = 0;
        //Calls helper function to calculate total tuples - empty slots
        numTuples = heapPage.availibleTuples();
    }
         
    public boolean hasNext() {
        if(currTuple < numTuples)
            return true;
        return false;
    }
        
    public Tuple next() {
        return heapPage.tuples[currTuple++];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Unimplemented, can't remove");
    }
}
