package simpledb;
import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {

    private HeapPage heapPage; 
    private int index;
    private int size;


    public HeapPageIterator(HeapPage heapPage) {
        this.heapPage = heapPage;
        this.index = 0;
        this.size = heapPage.getNumNESlots();
    }

    public boolean hasNext() {
        return (this.index < this.size) && (heapPage.tuples[this.index] != null);
    }

    public Tuple next() {
        return heapPage.tuples[this.index++];
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
}


