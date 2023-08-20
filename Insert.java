package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean called;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.called = false;

      	Type[] typeArray = new Type[] {Type.INT_TYPE}; // int type because we will return how many tuples have been inserted
        String[] stringArray = new String[] {"Inserted Tuples"};
      	this.td = new TupleDesc(typeArray, stringArray);

      	if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(this.tableId))) 
          throw new DbException("child TupleDesc differs from table for insertion");
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      // if called more than once
      if (this.called)  return null;

      int count = 0;

      // while we have tuples to insert
      while (this.child.hasNext()){
        try {
            Database.getBufferPool().insertTuple(this.t, this.tableId, this.child.next());
        }
        catch (IOException e) {
          throw new DbException("IO exception while inserting tuple");
        }
        count++;
      }

      // return how many tuples we have inserted as a confirmation
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
