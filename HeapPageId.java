package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	
	private int hp_tid;
	private int hp_pgno;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
    	hp_tid = tableId;
    	hp_pgno = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
    	return hp_tid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
    	return hp_pgno;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	String hp_hash = "" + hp_tid  + hp_pgno; //concatenating strings here
    	return hp_hash.hashCode(); //using the String hashCode() function 
        }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
    	if(o instanceof PageId) { //check if o is PageId
    		PageId pg_id = (PageId) o; //typecast
    		return (this.getTableId() == pg_id.getTableId() && this.getPageNumber() == pg_id.getPageNumber());
    	}
    	
    	return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}


