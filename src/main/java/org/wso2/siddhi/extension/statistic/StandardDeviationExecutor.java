package org.wso2.siddhi.extension.statistic;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by buddhi on 12/31/16.
 */
public class StandardDeviationExecutor extends FunctionExecutor {

    private StandardDeviationExecutor standardDeviationExecutor;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Standard Deviation aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type vType = attributeExpressionExecutors[0].getReturnType();
        switch (vType) {
            case FLOAT:
                standardDeviationExecutor = new StandardDeviationExecutorFloat();
                break;
            case INT:
                standardDeviationExecutor = new StandardDeviationExecutorInt();
                break;
            case LONG:
                standardDeviationExecutor = new StandardDeviationExecutorLong();
                break;
            case DOUBLE:
                standardDeviationExecutor = new StandardDeviationExecutorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Standard Deviation not supported for " + vType);
        }

    }

    @Override
    protected Object execute(Object[] objects) {
        return new IllegalStateException("Standard deviation cannot process");
    }

    @Override
    protected Object execute(Object object) {
        return standardDeviationExecutor.execute(object);
    }

    @Override
    public Attribute.Type getReturnType() {
        return standardDeviationExecutor.getReturnType();
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
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {
//        standardDeviationExecutor.restoreState(objects);
    }


    private class StandardDeviationExecutorDouble extends StandardDeviationExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            double inputValue = (Double) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

        @Override
        public void restoreState(Object[] objects) {
            sum=0.0;
            sumOfSquares=0.0;
            eventCounter=0;
        }



    }

    private class StandardDeviationExecutorLong extends StandardDeviationExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            double inputValue = (Long) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

//        @Override
//        public void restoreState(Object[] objects) {
//            sum=0.0;
//            sumOfSquares=0.0;
//            eventCounter=0;
//        }

    }

    private class StandardDeviationExecutorInt extends StandardDeviationExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            double inputValue = (Integer) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

//        @Override
//        public void restoreState(Object[] objects) {
//            sum=0.0;
//            sumOfSquares=0.0;
//            eventCounter=0;
//        }

    }

    private class StandardDeviationExecutorFloat extends StandardDeviationExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;


        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            double inputValue = (Float) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            return Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));
        }

//        @Override
//        public void restoreState(Object[] objects) {
//            sum=0.0;
//            sumOfSquares=0.0;
//            eventCounter=0;
//        }

    }

}
