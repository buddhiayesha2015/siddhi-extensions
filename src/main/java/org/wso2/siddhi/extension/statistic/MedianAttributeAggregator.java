package org.wso2.siddhi.extension.statistic;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.*;
/**
 * Created by buddhi on 1/17/17.
 */

public class MedianAttributeAggregator extends AttributeAggregator {

    private MedianAttributeAggregator medianAttributeAggregator;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length != 4) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 4 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type vType = attributeExpressionExecutors[3].getReturnType();
        switch (vType) {
            case FLOAT:
                medianAttributeAggregator = new MedianAttributeAggregatorFloat();
                break;
            case INT:
                medianAttributeAggregator = new MedianAttributeAggregatorInt();
                break;
            case LONG:
                medianAttributeAggregator = new MedianAttributeAggregatorLong();
                break;
            case DOUBLE:
                medianAttributeAggregator = new MedianAttributeAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Median not supported for " + vType);
        }

    }

    @Override
    public Attribute.Type getReturnType() {
        return medianAttributeAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object o) {

        return null;
    }

    @Override
    public Object processAdd(Object[] objects) {
        return null;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        return null;
    }

    @Override
    public Object reset() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }


    private class MedianAttributeAggregatorDouble extends MedianAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private ArrayList<Comparable> processList= new ArrayList<>();

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            processList.add((Double) data);
            return (Double) processList.get((Integer) getMedian(processList));
        }

        @Override
        public Object processRemove(Object o) {
            return null;
        }

    }

    private class MedianAttributeAggregatorLong extends MedianAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.LONG;
        private ArrayList<Comparable> processList= new ArrayList<>();

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            processList.add((Long) data);
            return (Long) processList.get((Integer) getMedian(processList));
        }

        @Override
        public Object processRemove(Object o) {
            return null;
        }

    }

    private class MedianAttributeAggregatorInt extends MedianAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private ArrayList<Comparable> processList= new ArrayList<>();

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            processList.add((Integer) data);
            return (Integer) processList.get((Integer) getMedian(processList));
        }

        @Override
        public Object processRemove(Object o) {
            return null;
        }

    }

    private class MedianAttributeAggregatorFloat extends MedianAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private ArrayList<Comparable> processList= new ArrayList<>();

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            processList.add((Float) data);
            return (Float) processList.get((Integer) getMedian(processList));
        }

        @Override
        public Object processRemove(Object o) {
            return null;
        }

    }

    //start code
    //median of medians method
    public static Comparable getMedian(ArrayList<Comparable> list) {
        int s = list.size();
        if (s < 1)
            throw new IllegalArgumentException();
        int pos = select(list, 0, s, s / 2);
        return list.get(pos);
    }

    /**
     * Returns position of k'th largest element of sub-list.
     *
     * @param list list to search, whose sub-list may be shuffled before
     *            returning
     * @param lo first element of sub-list in list
     * @param hi just after last element of sub-list in list
     * @param k
     * @return position of k'th largest element of (possibly shuffled) sub-list.
     */
    public static int select(ArrayList<Comparable> list, int lo, int hi, int k) {
        if (lo >= hi || k < 0 || lo + k >= hi)
            throw new IllegalArgumentException();
        if (hi - lo < 10) {
            Collections.sort(list.subList(lo, hi));
            return lo + k;
        }
        int s = hi - lo;
        int np = s / 5; // Number of partitions
        for (int i = 0; i < np; i++) {
            // For each partition, move its median to front of our sublist
            int lo2 = lo + i * 5;
            int hi2 = (i + 1 == np) ? hi : (lo2 + 5);
            int pos = select(list, lo2, hi2, 2);
            Collections.swap(list, pos, lo + i);
        }

        // Partition medians were moved to front, so we can recurse without making another list.
        int pos = select(list, lo, lo + np, np / 2);

        // Re-partition list to [<pivot][pivot][>pivot]
        int m = triage(list, lo, hi, pos);
        int cmp = lo + k - m;
        if (cmp > 0)
            return select(list, m + 1, hi, k - (m - lo) - 1);
        else if (cmp < 0)
            return select(list, lo, m, k);
        return lo + k;
    }

    /**
     * Partition sub-list into 3 parts [<pivot][pivot][>pivot].
     *
     * @param list
     * @param lo
     * @param hi
     * @param pos input position of pivot value
     * @return output position of pivot value
     */
    private static int triage(ArrayList<Comparable> list, int lo, int hi, int pos) {
        Comparable pivot = list.get(pos);
        int lo3 = lo;
        int hi3 = hi;
        while (lo3 < hi3) {
            Comparable e = list.get(lo3);
            int cmp = e.compareTo(pivot);
            if (cmp < 0)
                lo3++;
            else if (cmp > 0)
                Collections.swap(list, lo3, --hi3);
            else {
                while (hi3 > lo3 + 1) {
                    assert (list.get(lo3).compareTo(pivot) == 0);
                    e = list.get(--hi3);
                    cmp = e.compareTo(pivot);
                    if (cmp <= 0) {
                        if (lo3 + 1 == hi3) {
                            Collections.swap(list, lo3, lo3 + 1);
                            lo3++;
                            break;
                        }
                        Collections.swap(list, lo3, lo3 + 1);
                        assert (list.get(lo3 + 1).compareTo(pivot) == 0);
                        Collections.swap(list, lo3, hi3);
                        lo3++;
                        hi3++;
                    }
                }
                break;
            }
        }
        assert (list.get(lo3).compareTo(pivot) == 0);
        return lo3;
    }
    //end code


}
