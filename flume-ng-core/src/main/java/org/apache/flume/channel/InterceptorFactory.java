package org.apache.flume.channel;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InterceptorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(
            InterceptorFactory.class);

    public static List<Interceptor> getInterceptors(Context context) {
        List<Interceptor> interceptors = Lists.newLinkedList();

        String interceptorListStr = context.getString("interceptors", "");
        if (interceptorListStr.isEmpty()) {
            return null;
        }
        String[] interceptorNames = interceptorListStr.split("\\s+");

        Context interceptorContexts =
                new Context(context.getSubProperties("interceptors."));

        // run through and instantiate all the interceptors specified in the Context
        InterceptorBuilderFactory factory = new InterceptorBuilderFactory();
        for (String interceptorName : interceptorNames) {
            Context interceptorContext = new Context(
                    interceptorContexts.getSubProperties(interceptorName + "."));
            String type = interceptorContext.getString("type");
            if (type == null) {
                LOG.error("Type not specified for interceptor " + interceptorName);
                throw new FlumeException("Interceptor.Type not specified for " +
                        interceptorName);
            }
            try {
                Interceptor.Builder builder = factory.newInstance(type);
                builder.configure(interceptorContext);
                interceptors.add(builder.build());
            } catch (ClassNotFoundException e) {
                LOG.error("Builder class not found. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not found.", e);
            } catch (InstantiationException e) {
                LOG.error("Could not instantiate Builder. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not constructable.", e);
            } catch (IllegalAccessException e) {
                LOG.error("Unable to access Builder. Exception follows.", e);
                throw new FlumeException("Unable to access Interceptor.Builder.", e);
            }
        }
        return interceptors;
    }
}