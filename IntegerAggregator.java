package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private HashMap<Field,Integer> aggregate;
    private HashMap<Field,Integer> count;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
      gbField = gbfield;
    	gbFieldType = gbfieldtype;
    	aField = afield;
    	op = what;
    	aggregate = new HashMap<Field, Integer>();
    	count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
      Field tupleGroupByField;

      // do we have a field to group by or not?
      if (gbField == Aggregator.NO_GROUPING)
        tupleGroupByField = null;
      else
        tupleGroupByField = tup.getField(gbField);

      // if we don't have the key in the hashMaps, create initial values in the hashMaps
      if (!aggregate.containsKey(tupleGroupByField)){
        int initial;
        switch(op){
          case MIN: initial = Integer.MAX_VALUE; break; // start with max and update as we find min
          case MAX: initial = Integer.MIN_VALUE; break; // start with min and update as we find max
          default: initial = 0; break; // other operators
        }
    		aggregate.put(tupleGroupByField, initial); // aggregate depends on op
        count.put(tupleGroupByField, 0); // count is 0
    	}

      int tupleValue = ((IntField) tup.getField(aField)).getValue();
    	int currentValue = aggregate.get(tupleGroupByField);
    	int currentCount = count.get(tupleGroupByField);
    	int newValue = currentValue;

      // update the value to put in the aggregate hashMap based on what op we are working with
    	switch(op){
    		case MIN:
          if (tupleValue < currentValue) // update
            newValue = tupleValue;
          else
            newValue = currentValue;
    			break;
    		case MAX:
          if (tupleValue > currentValue) // update
            newValue = tupleValue;
          else
            newValue = currentValue;
    			break;
        case COUNT:
          newValue = currentValue + 1;
          break;
    		case SUM: case AVG:
          // update count hashMap so we can calculate sum or avg at the end
    			count.put(tupleGroupByField, currentCount+1);
    			newValue = tupleValue + currentValue;
    			break;
  			default:
  				break;
    	}
    	aggregate.put(tupleGroupByField, newValue); // update aggregate hashMap
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */

    public OpIterator iterator() {
      ArrayList<Tuple> tuples = new ArrayList<Tuple>();

      String[] names;
    	Type[] types;
    	if (gbField == Aggregator.NO_GROUPING){ // no grouping so just the aggregate value
        types = new Type[] {Type.INT_TYPE};
    		names = new String[] {"aggregateValue"};
    	} else { // grouping so aggregate value per groupby field type
        types = new Type[] {gbFieldType, Type.INT_TYPE};
    		names = new String[] {"groupValue", "aggregateValue"};
    	}
    	TupleDesc td = new TupleDesc(types, names);

    	Tuple tupleToAdd;
    	for (Field group : aggregate.keySet()){
    		int aggregateValue;
    		if (op == Op.AVG) // if we want AVG we have to calculate it here by doing aggregate / count
    			aggregateValue = aggregate.get(group) / count.get(group);
    		else
    			aggregateValue = aggregate.get(group);

    		tupleToAdd = new Tuple(td);
    		if (gbField == Aggregator.NO_GROUPING) // no grouping so just the aggregate value
    			tupleToAdd.setField(0, new IntField(aggregateValue));
    		else { // grouping so groupby value and aggregate value
      		tupleToAdd.setField(0, group);
      		tupleToAdd.setField(1, new IntField(aggregateValue));
    		}
    		tuples.add(tupleToAdd);
    	}
    	return new TupleIterator(td, tuples);
    }
}
