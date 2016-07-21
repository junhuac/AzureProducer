package azure;

import java.util.*;

import java.io.IOException;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Producer implements RequestHandler<RequestClass, ResponseClass> {
    public ResponseClass handleRequest(RequestClass request, Context context){
   
        final String namespaceName = "dtna-ns";
        final String eventHubName = "dtna";
        final String sasKeyName = "zonar";
        final String sasKey = "qb5otAE6g7vyyU60uBHbWS50B8+xBK7oeW+gUO9rixc=";

        String connectionString = "Endpoint=sb://dtna-ns.servicebus.windows.net/;SharedAccessKeyName=zonar;SharedAccessKey=qb5otAE6g7vyyU60uBHbWS50B8+xBK7oeW+gUO9rixc=;EntityPath=dtna";
        ConnectionStringBuilder connStr = new ConnectionStringBuilder(connectionString);

        byte[] payloadBytes = (request.getDeviceid() + " " + request.getLatitude() + " " + request.getLongitude()).getBytes(Charset.defaultCharset());
        EventData sendEvent = new EventData(payloadBytes);

        EventHubClient ehClient;

        try
        {
            ehClient = EventHubClient.createFromConnectionString(connStr.toString()).get();
            ehClient.send(sendEvent).get();
            ehClient.close();
        }
        catch (ServiceBusException exception)
        {
            return new ResponseClass("ServiceBusException caught!");
        }
        catch (IOException exception)
        {
            return new ResponseClass("IOException caught!");
        }
        catch (InterruptedException exception)
        {
            return new ResponseClass("InterruptedException caught!");
        }
        catch (ExecutionException exception)
        {
            return new ResponseClass("ExecutionException caught!");
        }

	return new ResponseClass(request.getDeviceid() + " " + request.getLatitude() + " " + request.getLongitude());        
    
    }
}
