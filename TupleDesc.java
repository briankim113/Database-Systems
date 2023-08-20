package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Vector<TDItem> td_Vector; //using vector data structure to store the TD items
    public Iterator<TDItem> iterator() {
        return td_Vector.iterator(); 
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    //constructor
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if(typeAr.length>=1 && typeAr.length == fieldAr.length) {
			td_Vector = new Vector<TDItem>(typeAr.length); // create vector of the size
	        for (int i = 0; i < typeAr.length; i++){ 
	        	td_Vector.add(new TDItem(typeAr[i], fieldAr[i])); // loop over the vector and fill the vector with appropriate data
	        }
    	}
}


    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields --> we will use NULL for this.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	if(typeAr.length>=1) {
	    	td_Vector = new Vector<TDItem>(typeAr.length); // create vector of the size
	        for (int i = 0; i < typeAr.length; i++){ 
	        	td_Vector.add(new TDItem(typeAr[i], null)); // loop over the vector and fill the vector with appropriate data
	        }
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return td_Vector.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i<0 || i>=numFields()){ //if out of bounds, throw exception
    		throw new NoSuchElementException();
    	}
    	
        return td_Vector.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i<0 || i>=numFields()){ //if out of bounds, throw exception
    		throw new NoSuchElementException();
    	}
    	
        return td_Vector.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null) { //if the given name passed as parameter is a NULL 
    		throw new NoSuchElementException("field name is null"); 
    	}
   	
    	if (name instanceof String)
    	{
    		boolean allNULL = true;
	    	//boolean found = false;
	    	for (int i = 0; i < numFields(); i++) {
	    		if (td_Vector.get(i).fieldName == null)
	    			continue;
	    		allNULL = false;
	    		if (td_Vector.get(i).fieldName.equals(name)) {
	    			//found = true;
	    			return i;
	    		}
	    	}
	    	
	    	if (allNULL)
	    		throw new NoSuchElementException("all fields are null"); 
	    		
//	    	if (!found){
//	    		throw new NoSuchElementException(); 
//	    	}
    	}
   
    	throw new NoSuchElementException();  	
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (int i = 0; i < numFields(); i++) {
    		size += td_Vector.get(i).fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc --> since it says 'new', we will create a new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int newNumFields = td1.numFields() + td2.numFields();
    	Type[] typeAr = new Type[newNumFields];
    	String[] fieldAr = new String[newNumFields];
    	
    	for (int i = 0; i < td1.numFields(); i++) {
    		typeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	
    	for (int i = 0; i < td2.numFields(); i++) {
    		typeAr[i+td1.numFields()] = td2.getFieldType(i);
    		fieldAr[i+td1.numFields()] = td2.getFieldName(i);
    	}
    	
    	//alternative approach
//    	for (int i = td1.numFields(); i < newNumFields; i++) {
//    		typeAr[i] = td2.getFieldType(i-td1.numFields());
//    		fieldAr[i] = td2.getFieldName(i-td1.numFields());
//    	}
    	
    	return new TupleDesc(typeAr, fieldAr);    	
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	// first verify that object is TupleDesc --> use instanceof for this
    	// first check if they have the same number of TDItems
    	// then check if every type is the same in the same order
    	// then it's equal
    	// myTupleDesc.equals(otherTupleDesc) --> true / false
    	
    	if (o == null) {
    		return false;
    	}
    	
    	if (!(o instanceof TupleDesc)) {
    		return false;
    	}
    	
    	//if it is an instance of TupleDesc, then type cast it so that we can use its functions/methods
    	TupleDesc o_td = (TupleDesc)o;
    	
    	//if the number of fields aren't the same
    	if (this.numFields() != o_td.numFields()) {
    		return false;
    	}
    	
    	//if the i-th entries aren't of the same type
    	for (int i = 0; i < this.numFields(); i++) {
    		if (this.getFieldType(i) != o_td.getFieldType(i)) {
    			return false;
    		}
    	}
    	
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	// iterate through the TDITEM vector
    	// and use the tditem.toString() method
    	String description = "";
    	for (int i = 0; i < this.numFields(); i++) {
    		 description += (td_Vector.get(i).toString() + ", ");
    	}
    	
        return description;
    }
}


