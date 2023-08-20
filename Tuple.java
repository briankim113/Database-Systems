package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    //this was not given in the code - check if u can create it
    private RecordId _rid; //to maintain record id
    private TupleDesc _td; //to define the descriptor of this tuple
    private ArrayList<Field> _fields; //for the actual fields
    //using an arrayList for this rather than linked list because accessing random fields/columns will occur more often that adding/deleting fields/columns
    //using array list instead of array to keep the possibility of changing the size later
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    //constructor
    public Tuple(TupleDesc td) {
    	assert (td instanceof TupleDesc); //to check if it is a valid TupleDesc instance
    	assert (td.numFields() > 0); //to check if it has at least one field
    	_td = td; //assign the descriptor
    	_fields = new ArrayList<>(td.numFields()); //create as many field columns as there are in td
    	for (int i = 0; i < td.numFields(); i++) 
            _fields.add(null);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return _td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return _rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        _rid = rid; 
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) throws NoSuchElementException {
    	if (i<0 || i>=_td.numFields()){ //if out of bounds, throw exception
    		throw new NoSuchElementException("Index Out Of Bounds \n");
    	}
    	
    	if(f.getType().equals(_td.getFieldType(i))) //if field type matches
    		_fields.set(i,  f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	if (i<0 || i>=_td.numFields()){ //if out of bounds, throw exception
    		throw new NoSuchElementException("Index Out Of Bounds \n");
    	}
    	
		return _fields.get(i); //taken care of null in the constructor
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
    	String s = "";
    	for (int i = 0; i < _fields.size()-1; i++){
    		s += _fields.get(i) + "\t";
    	}
    	s += _fields.get(_fields.size()-1) + "\n"; //for the last field
    	return s;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	 return _fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	_td = td;
        }
}


