package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private boolean called;
    private TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;

        Type[] typeArray = new Type[] {Type.INT_TYPE}; // int type because we will return how many tuples have been deleted
        String[] stringArray = new String[] {"Deleted Tuples"};
    	  td = new TupleDesc(typeArray, stringArray);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // similar to Insert
        if (this.called) return null;
      	int count = 0;

        // while we have tuples to delete
      	while (this.child.hasNext()){
      		try {
          		Database.getBufferPool().deleteTuple(this.t, this.child.next());
          } catch (IOException e) {
      			throw new DbException("IO Exception while deleting tuple");
      		}

          count++;
      	}

        // return how many tuples we have deleted as a confirmation
      	Tuple result = new Tuple(this.td); // (typeArray, stringArray) from above
      	result.setField(0, new IntField(count));
      	this.called = true;

      	return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
