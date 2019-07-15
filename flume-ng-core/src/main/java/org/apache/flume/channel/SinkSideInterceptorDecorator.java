package org.apache.flume.channel;

import org.apache.flume.*;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorChain;
import org.apache.flume.lifecycle.LifecycleState;

import java.util.List;

public class SinkSideInterceptorDecorator implements Channel {
    private final Channel channel;
    private final InterceptorChain interceptorChain = new InterceptorChain();

    public SinkSideInterceptorDecorator(Channel channel, Context context) {
        this.channel = channel;
        List<Interceptor> interceptors = InterceptorFactory.getInterceptors(context);
        if (interceptors != null) {
            interceptorChain.setInterceptors(interceptors);
        }
    }

    @Override
    public void put(Event event) throws ChannelException {
        channel.put(event);
    }

    @Override
    public Event take() throws ChannelException {
        return interceptorChain.intercept(channel.take());
    }

    @Override
    public Transaction getTransaction() {
        return channel.getTransaction();
    }

    @Override
    public void setName(String name) {
        channel.setName(name);
    }

    @Override
    public String getName() {
        return channel.getName();
    }

    @Override
    public void start() {
        channel.start();
    }

    @Override
    public void stop() {
       channel.stop();
    }

    @Override
    public LifecycleState getLifecycleState() {
        return channel.getLifecycleState();
    }
}
