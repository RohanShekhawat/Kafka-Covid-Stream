
package com.sid.kafka;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class ParseTwitterStreamTask implements StreamTask  {

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

        String msg = ((String) envelope.getMessage());

        try {
            JSONParser parser  = new JSONParser();
            Object     obj     = parser.parse(msg);
            JSONObject jsonObj = (JSONObject) obj;
            String     text    = (String) jsonObj.get("text");
            if(text == null ||text.trim().isEmpty() ) {
                text = "found text == null ||text.trim().isEmpty " ;
            }

            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "tweets-parsed"), text));
        } catch (ParseException pe) {}


    }
}
