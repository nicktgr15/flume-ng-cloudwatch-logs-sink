package com.nicktgr15;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CloudwatchLogsSink extends AbstractSink implements Configurable {

    private AWSLogsClient awsLogsClient;
    private static final Log logger = LogFactory.getLog(CloudwatchLogsSink.class);
    private String sequenceToken = null;
    private int batchSizeLimit = 32768;
    private int batchEventsLimit = 1000;
    private Event bufferedEvent;
    private String logGroupName;
    private String logStreamName;
    private String awsAccessKeyId;
    private String awsSecretAccessKey;
    private int totalEventsSent = 0;


    @Override
    public void configure(Context context) {
        this.logGroupName = context.getString("logGroupName");
        this.logStreamName = context.getString("logStreamName");
        this.awsAccessKeyId = context.getString("awsAccessKeyId");
        this.awsSecretAccessKey = context.getString("awsSecretAccessKey");
    }

    @Override
    public void start() {
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
        awsLogsClient = new AWSLogsClient(basicAWSCredentials);

        DescribeLogGroupsRequest describeLogGroupsRequest = new DescribeLogGroupsRequest();
        describeLogGroupsRequest.setLogGroupNamePrefix(logGroupName);
        DescribeLogGroupsResult describeLogGroupsResult = awsLogsClient.describeLogGroups(describeLogGroupsRequest);

        if (describeLogGroupsResult.getLogGroups().size() == 0) {
            CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest(logGroupName);
            awsLogsClient.createLogGroup(createLogGroupRequest);
        }

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest();
        describeLogStreamsRequest.setLogGroupName(logGroupName);
        describeLogStreamsRequest.setLogStreamNamePrefix(logStreamName);
        DescribeLogStreamsResult describeLogStreamsResult = awsLogsClient.describeLogStreams(describeLogStreamsRequest);

        if (describeLogStreamsResult.getLogStreams().size() == 0) {
            CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest(logGroupName, logStreamName);
            awsLogsClient.createLogStream(createLogStreamRequest);
        } else {
            sequenceToken = describeLogStreamsResult.getLogStreams().get(0).getUploadSequenceToken();
        }
    }

    @Override
    public void stop () {
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        try {

            txn.begin();

            int batchSize = 0;
            List<InputLogEvent> logEventList = new ArrayList<InputLogEvent>();

            for(int i = 0; i < batchEventsLimit; i++){
                Event event;

                // if we have a buffered event take that first
                if(bufferedEvent!=null){
                    event = bufferedEvent;
                    bufferedEvent = null;
                } else {
                    event = channel.take();
                }

                if (event == null) {
                    break;
                }

                // if the batchSize becomes bigger than 32 bytes (26 is the overhead)
                if(batchSize + event.getBody().length + 26 > batchSizeLimit){
                    bufferedEvent = event;
                    break;
                }
                batchSize += event.getBody().length + 26;

                InputLogEvent inputLogEvent = new InputLogEvent();
                inputLogEvent.setMessage(new String(event.getBody(), "UTF-8"));
                Date dt = new Date();
                long t = dt.getTime();
                inputLogEvent.setTimestamp(t);
                logEventList.add(inputLogEvent);
            }

            if (logEventList.size() == 0) {
                status = Status.BACKOFF;
            } else {
                PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest();
                putLogEventsRequest.setLogGroupName(logGroupName);
                putLogEventsRequest.setLogStreamName(logStreamName);
                putLogEventsRequest.setLogEvents(logEventList);

                if (sequenceToken == null) {
                    PutLogEventsResult putLogEventsResult = awsLogsClient.putLogEvents(putLogEventsRequest);
                    sequenceToken = putLogEventsResult.getNextSequenceToken();
                } else {
                    putLogEventsRequest.setSequenceToken(sequenceToken);
                    PutLogEventsResult putLogEventsResult = awsLogsClient.putLogEvents(putLogEventsRequest);
                    sequenceToken = putLogEventsResult.getNextSequenceToken();
                }
                status = Status.READY;
                totalEventsSent += logEventList.size();
                logger.info(totalEventsSent);
            }
            txn.commit();

        } catch (Throwable t) {
            txn.rollback();
            logger.error("error", t);

            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }
}