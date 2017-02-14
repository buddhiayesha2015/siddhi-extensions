package org.wso2.siddhi.extension.statistic;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by Buddhi on 2/15/2017.
 */

public class ZScoreExecutor extends FunctionExecutor {

    private ZScoreExecutor zScoreExecutor;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Z-Score Executor has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type vType = attributeExpressionExecutors[0].getReturnType();
        switch (vType) {
            case FLOAT:
                zScoreExecutor = new ZScoreExecutorFloat();
                break;
            case INT:
                zScoreExecutor = new ZScoreExecutorInt();
                break;
            case LONG:
                zScoreExecutor = new ZScoreExecutorLong();
                break;
            case DOUBLE:
                zScoreExecutor = new ZScoreExecutorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Z-Score Executor not supported for " + vType);
        }

    }

    @Override
    protected Object execute(Object[] objects) {
        return new IllegalStateException("Z-Score Executor cannot process");
    }

    @Override
    protected Object execute(Object object) {
        return zScoreExecutor.execute(object);
    }

    @Override
    public Attribute.Type getReturnType() {
        return zScoreExecutor.getReturnType();
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

    }


    private class ZScoreExecutorDouble extends ZScoreExecutor {

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

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
        }

        @Override
        public void restoreState(Object[] objects) {

        }


    }

    private class ZScoreExecutorLong extends ZScoreExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            long inputValue = (Long) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
        }

        @Override
        public void restoreState(Object[] objects) {

        }


    }

    private class ZScoreExecutorInt extends ZScoreExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            int inputValue = (Integer) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
        }

        @Override
        public void restoreState(Object[] objects) {

        }


    }

    private class ZScoreExecutorFloat extends ZScoreExecutor {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum=0.0, sumOfSquares=0.0;
        private long eventCounter=0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object execute(Object data) {

            float inputValue = (Float) data;
            eventCounter++;
            sum += inputValue;
            sumOfSquares += inputValue*inputValue;

            double mean = sum / eventCounter;
            double sd = Math.sqrt((sumOfSquares / eventCounter) - Math.pow(sum / eventCounter, 2.0));

            return (inputValue - mean) / sd;
        }

        @Override
        public void restoreState(Object[] objects) {

        }


    }

}
