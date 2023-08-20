package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId rid_pid;
    private Integer rid_tnum; //Integer used so we can convert to string for hashcode later

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    //constructor
    public RecordId(PageId pid, int tupleno) {
    	rid_pid = pid;
    	rid_tnum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
    	return rid_tnum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return rid_pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if(o == this) {
            return true;
        }
        	if ((o == null && this != null) || (o != null && this == null)) {
            return false;
        }
    	
    	if(o instanceof RecordId) { //check if o is RecordId
    		RecordId rec_id  = (RecordId) o; //typecast
    		
    		return (this.rid_pid.equals(rec_id.rid_pid) && this.rid_tnum == rec_id.rid_tnum);
    	}

    	return false;

    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	String rid_hash = this.rid_pid.toString() + this.rid_tnum.toString(); //using PageId and Integer functions to convert to strings 
    	return rid_hash.hashCode(); //using String hashcode function

    }

}


