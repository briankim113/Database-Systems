package simpledb;

import java.util.*;

class HeapFileIterator extends AbstractDbFileIterator {
    //AbstractDbFileIterator already has hasNext(), next(), and close()
    //Needs readNext() function implemented

    HeapFile heapFile;
    TransactionId tid;
    int curr_pgNo;
    Iterator<Tuple> tupleIter;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {            
        heapFile = hf;
        this.tid = tid;
    }

    public void open() throws DbException, TransactionAbortedException {
        curr_pgNo = -1;
    }

    // from AbstractDbFileIterator
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        
       //if current tupleIter no longer has a tuple to read next
       if (tupleIter != null && !tupleIter.hasNext()) {    
           tupleIter = null;
        }

        // open a new tuple iterator for the new page until the file runs out of pages
        while (tupleIter == null && curr_pgNo < heapFile.numPages() - 1 && curr_pgNo > -2) {
            // go to next page in the file
            curr_pgNo++;
            
            // get the tuple iterator for the current page
            HeapPageId curr_pgId = new HeapPageId(heapFile.getId(), curr_pgNo);
            HeapPage curr_pg = (HeapPage) Database.getBufferPool().getPage(tid, curr_pgId, Permissions.READ_ONLY);
            tupleIter = curr_pg.iterator();

	// uncomment to check how many tuples are actually being saved
            // System.out.println(curr_pg.getNumNESlots());

            
            // does this page have tuples? if not, we need to find the next one
            if (!tupleIter.hasNext())
                tupleIter = null;

            // otherwise, we can leave the while loop b/c we found the next page with a tupleIter
        }
        
        // if we never found one, then return null
        if (tupleIter == null)
                return null;

        
        // return the next tuple that we know we have
        return tupleIter.next();
    }

    /**
     * Rewind closes the current iterator and then opens it again.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Close the iterator, which resets the counters so it can be opened again.
     */
    public void close() {
        super.close(); // from AbstractDbFileIterator
        tupleIter = null;
        curr_pgNo = Integer.MIN_VALUE;

    }
}



