package simpledb;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator tupleIter;
    private int aField;
    private int gbField;
    private Aggregator.Op op;
    private OpIterator aggregateIter;
    private Aggregator aggregator;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
      // assign values to attributes
      tupleIter = child;
    	aField = afield;
    	gbField = gfield;
    	op = aop;
    	aggregateIter = null;

    	Type groupByType;
    	if (gbField == Aggregator.NO_GROUPING) // no grouping, so no groupByType either
    		groupByType = null;
    	else // yes grouping, so give it a groupBy type
    		groupByType = tupleIter.getTupleDesc().getFieldType(gbField);
    	Type aggregateType = tupleIter.getTupleDesc().getFieldType(aField);

      // create an aggregator based on the type
    	switch(aggregateType){
    		case INT_TYPE:
    			aggregator = new IntegerAggregator(gbField, groupByType, aField, op);
    			break;
    		case STRING_TYPE:
    			aggregator = new StringAggregator(gbField, groupByType, aField, op);
    			break;
        default: // illegal aggregateType if not int or string
          throw new IllegalArgumentException();
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return gbField; // how to return Aggregator.NO_GROUPING if it's not accompanied by a groupby?
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
      if (groupField() == Aggregator.NO_GROUPING)
        return null;

    	return tupleIter.getTupleDesc().getFieldName(groupField());
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
      return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
      return tupleIter.getTupleDesc().getFieldName(aggregateField());
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	     return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	     return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // exceptions are thrown in these respective functions like .open(), .hasNext()
        super.open(); // Operator.open();
      	tupleIter.open();

        // now get all the tuples and aggregate them
      	while (tupleIter.hasNext()){
      		aggregator.mergeTupleIntoGroup(tupleIter.next());
      	}

        // and open aggregateIter to be able to go through the filled data
      	aggregateIter = aggregator.iterator();
      	aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      // same comment as above about exception handling
      if (aggregateIter.hasNext())
        return aggregateIter.next();
      else
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
      // same comment as above about exception handling
      aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	return tupleIter.getTupleDesc();
    }

    public void close() {
      super.close(); // Operator.close();
    	tupleIter.close();
    	aggregateIter.close();
    }

    // below functions are override implementations from Operator functions

    // If there is only one child, return an array of only one element.
    @Override
    public OpIterator[] getChildren() {
      return new OpIterator[] {aggregateIter};
    }

    // Set the children(child) of this operator. If the operator has only one child, children[0] should be used.
    @Override
    public void setChildren(OpIterator[] children) {
      aggregateIter = children[0];
    }

}
