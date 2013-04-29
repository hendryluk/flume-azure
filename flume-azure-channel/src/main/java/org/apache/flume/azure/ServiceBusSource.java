package org.apache.flume.azure;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.windowsazure.services.core.Configuration;
import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusContract;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusService;
import com.microsoft.windowsazure.services.serviceBus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveMessageResult;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveMode;

public class ServiceBusSource extends AbstractSource implements Configurable, PollableSource {

	private static final Logger log = LoggerFactory.getLogger(ServiceBusSource.class);
	private String _queueName;
	
	private String _namespace;
	private String _authName;
	private String _authPwd;
	private String _rootUri;
	private String _wrapRootUri;
	private ServiceBusContract _service;
	private ReceiveMessageOptions _opts;
	
	public void configure(Context context) {
		_queueName = context.getString(ServiceBusConstants.CONFIG_QUEUE_NAME);
		_namespace = context.getString(ServiceBusConstants.CONFIG_NAMESPACE);
		_authName = context.getString(ServiceBusConstants.CONFIG_AUTH_NAME);
		_authPwd = context.getString(ServiceBusConstants.CONFIG_AUTH_PWD);
		_rootUri = context.getString(ServiceBusConstants.CONFIG_ROOT_URI);
		_wrapRootUri = context.getString(ServiceBusConstants.CONFIG_WRAP_ROOT_URI);
		
		ensureConfigCompleteness(context);
	}
	
	private void ensureConfigCompleteness(Context context) {
		if(StringUtils.isEmpty(_queueName))
			throw new IllegalArgumentException("You must configure queue name");
		
		if(StringUtils.isEmpty(_namespace))
			throw new IllegalArgumentException("You must configure namespace");
		
		if(StringUtils.isEmpty(_authName))
			throw new IllegalArgumentException("You must configure auth name");
		
		if(StringUtils.isEmpty(_authPwd))
			throw new IllegalArgumentException("You must configure auth password");
		
		if(StringUtils.isEmpty(_rootUri))
			throw new IllegalArgumentException("You must configure root URI");
		
		if(StringUtils.isEmpty(_wrapRootUri))
			throw new IllegalArgumentException("You must configure WRAP root URI");
	}

	public Status process() throws EventDeliveryException {
		
		ReceiveMessageResult resultQm;
		try {
			resultQm = _service.receiveMessage(_queueName, _opts);
		} catch (ServiceException e) {
			throw new EventDeliveryException("Error receiving message from ServiceBus queue", e);
		}
		BrokeredMessage msg = resultQm.getValue();
		if(msg != null && msg.getMessageId() != null)
		{
			try {
				byte[] body;
				try{
					body = IOUtils.toByteArray(msg.getBody());
				} catch (IOException e) {
					throw new EventDeliveryException("Error reading message body from ServiceBus queue", e);
				}
				
				Map<String, String> headers = getHeaders(msg);
				
				Event event = new SimpleEvent();
				event.setBody(body);
				event.setHeaders(headers);
				getChannelProcessor().processEvent(event);
				
				try {
					_service.deleteMessage(msg);
				} catch (ServiceException e) {
					throw new EventDeliveryException("Error deleting processed message off the ServiceBus queue", e);
				}
				
				return Status.READY;
			}
			finally {
				try {
					_service.unlockMessage(msg);
				} catch (ServiceException e) {
					log.error("Error unlocking undelivered message from the ServiceBus queue", e);
				}
			}
		}
			
		return Status.BACKOFF;
	}
	
	private Map<String, String> getHeaders(BrokeredMessage msg)
	{
		Map<String, String> map = new HashMap<String, String>();
		for(String key: msg.getProperties().keySet())
		{
			map.put(key, String.valueOf(msg.getProperty(key)));
		}
		return map;
	}
	
	@Override
	public void start(){
		Configuration config = ServiceBusConfiguration.configureWithWrapAuthentication(_namespace, _authName, _authPwd, _rootUri, _wrapRootUri);
		_service = ServiceBusService.create(config);
		
		_opts = ReceiveMessageOptions.DEFAULT;
		_opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
		
		super.start();
	}
}
