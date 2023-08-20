package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate _p;
    private OpIterator _childIt;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        _p = p;
        _childIt = child;
    }

    public Predicate getPredicate() {
        return _p;
    }

    public TupleDesc getTupleDesc() {
        return _childIt.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open(); 
        _childIt.open();
    }

    public void close() {
    	super.close();
        _childIt.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _childIt.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	while (_childIt.hasNext()) {
            Tuple _tuple = _childIt.next();
            if (_p.filter(_tuple)) {
                return _tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
    	return new OpIterator[] {_childIt};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	if (_childIt != children[0]) {
            _childIt = children[0];
        }
    }

}

