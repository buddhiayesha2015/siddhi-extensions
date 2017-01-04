package org.wso2.siddhi.extension.statistic;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by buddhi on 12/31/16.
 */
public class StandardDeviationAggregator extends AttributeAggregator {

    private StandardDeviationAggregator standardDeviationAggregator;

    /**
     * The initialization method for AttributeAggregator
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Standard Deviation aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type vType = attributeExpressionExecutors[0].getReturnType();
        switch (vType) {
            case FLOAT:
                standardDeviationAggregator = new StandardDeviationAggregatorFloat();
                break;
            case INT:
                standardDeviationAggregator = new StandardDeviationAggregatorInt();
                break;
            case LONG:
                standardDeviationAggregator = new StandardDeviationAggregatorLong();
                break;
            case DOUBLE:
                standardDeviationAggregator = new StandardDeviationAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Standard Deviation not supported for " + vType);
        }

    }

    @Override
    public Attribute.Type getReturnType() {
        return standardDeviationAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return standardDeviationAggregator.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        return new IllegalStateException("Standard deviation cannot process");
    }

    @Override
    public Object processRemove(Object data) {
        return standardDeviationAggregator.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        return new IllegalStateException("Standard deviation cannot process");
    }

    @Override
    public Object reset() {
        return standardDeviationAggregator.reset();
    }

    @Override
    public void start() {
        // Nothing to start
    }

    @Override
    public void stop() {
        // Nothing to stop
    }

    @Override
    public Object[] currentState() {
        return null;
    }

    @Override
    public void restoreState(Object[] objects) {

    }


    private class StandardDeviationAggregatorDouble extends StandardDeviationAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            double inputValue = (Double) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

        @Override
        public Object processRemove(Object obj) {
            return null;
        }

        @Override
        public Object reset() {
            sum=0.0;
            sumOfSquares=0.0;
            eventCounter=0;
            return 0.0;
        }

    }

    private class StandardDeviationAggregatorLong extends StandardDeviationAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            double inputValue = (Long) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

        @Override
        public Object processRemove(Object obj) {
            return null;
        }

        @Override
        public Object reset() {
            sum=0.0;
            sumOfSquares=0.0;
            eventCounter=0;
            return 0.0;
        }

    }

    private class StandardDeviationAggregatorInt extends StandardDeviationAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            double inputValue = (Integer) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

        @Override
        public Object processRemove(Object obj) {
            return null;
        }

        @Override
        public Object reset() {
            sum=0.0;
            sumOfSquares=0.0;
            eventCounter=0;
            return 0.0;
        }

    }

    private class StandardDeviationAggregatorFloat extends StandardDeviationAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            double inputValue = (Float) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

        @Override
        public Object processRemove(Object obj) {
            return null;
        }

        @Override
        public Object reset() {
            sum=0.0;
            sumOfSquares=0.0;
            eventCounter=0;
            return 0.0;
        }

    }
    
}
