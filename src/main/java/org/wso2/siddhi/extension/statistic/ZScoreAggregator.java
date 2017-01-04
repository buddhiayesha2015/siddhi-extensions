package org.wso2.siddhi.extension.statistic;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by buddhi on 12/31/16.
 */
public class ZScoreAggregator extends AttributeAggregator {

    private ZScoreAggregator zScoreAggregator;

    /**
     * The initialization method for AttributeAggregator
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Z Score aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type vType = attributeExpressionExecutors[0].getReturnType();
        switch (vType) {
            case FLOAT:
                zScoreAggregator = new ZScoreAggregatorFloat();
                break;
            case INT:
                zScoreAggregator = new ZScoreAggregatorInt();
                break;
            case LONG:
                zScoreAggregator = new ZScoreAggregatorLong();
                break;
            case DOUBLE:
                zScoreAggregator = new ZScoreAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Z Score not supported for " + vType);
        }

    }

    @Override
    public Attribute.Type getReturnType() {
        return zScoreAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return zScoreAggregator.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        return new IllegalStateException("Z Score cannot process");
    }

    @Override
    public Object processRemove(Object data) {
        return zScoreAggregator.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        return new IllegalStateException("Z Score cannot process");
    }

    @Override
    public Object reset() {
        return zScoreAggregator.reset();
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


    private class ZScoreAggregatorDouble extends ZScoreAggregator {

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

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
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

    private class ZScoreAggregatorLong extends ZScoreAggregator {

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

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
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

    private class ZScoreAggregatorInt extends ZScoreAggregator {

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

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
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

    private class ZScoreAggregatorFloat extends ZScoreAggregator {

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

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
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
