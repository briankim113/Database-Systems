package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private HashMap<Field,Integer> count; //only COUNT is supported

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
      if (what != Op.COUNT)
        throw new IllegalArgumentException();

      gbField = gbfield;
      gbFieldType = gbfieldtype;
      aField = afield;
      op = what;
      count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
      Field tupleGroupByField;
      if (gbField == Aggregator.NO_GROUPING) // based on informatino provided in the constructor definition
        tupleGroupByField = null;
      else
        tupleGroupByField = tup.getField(gbField);

      // if we don't have the key, create initial value in the count hashMap
    	if (!count.containsKey(tupleGroupByField)){
    		count.put(tupleGroupByField, 0);
    	}

    	count.put(tupleGroupByField, count.get(tupleGroupByField)+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
      for (Field group : count.keySet()){
        int aggregateValue = count.get(group);
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
