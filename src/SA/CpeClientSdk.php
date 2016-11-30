<?php

/** 
 * Copyright (C) 2015, Sport Archive Inc.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License (version 3) as published by
 * the Free Software Foundation; 
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * For a complete version of the license, head to:
 * http://www.gnu.org/licenses/gpl-3.0.en.html
 * 
 * Cloud Processing Engine, Copyright (C) 2015, Sport Archive Inc
 * Cloud Processing Engine comes with ABSOLUTELY NO WARRANTY;
 * This is free software, and you are welcome to redistribute it
 * under certain conditions;
 * 
 * June 29th 2015
 * Sport Archive Inc.
 * info@sportarchive.tv 
 *
 */

namespace SA;

/**
 * The CpeClientSDK is to be used by client applications of the 
 * Client Processing Engine (CPE) project.
 * 
 * See: https://github.com/sportarchive/CloudProcessingEngine
 *
 * You must use this SDK in your PHP client application if you want to communicate 
 * with the CPE stack through AWS SQS.
 * 
 * **It allows you to:**
 *    - Poll SQS messages coming from the CPE stack
 *    - Send correctly crafted messages to the CPE Stack (commands such as `start_job`).
 *
 *
 * **Install:**
 * To install this SDK, just user composer.
 * 
 * See: https://packagist.org/packages/sportarchive/cloud-processing-engine-client-sdk-php
 */
class CpeClientSdk
{
    /** AWS region for SQS **/
    private $region;
    /** AWS SQS handler **/
    private $sqs;
    /** 
     * AWS STS handler.
     * 
     * You can use AWS roles with your EC2 instance instead of passing AWS credentials.
     */
    private $sts;
    /** Debug flag **/
    private $debug;
    
    // Msg Types
    const START_JOB          = "START_JOB";
    // FIXME: Add missing calls
    // const CANCEL_JOB         = "CANCEL_JOB";

    /**
     * Constructor for the CpeClientSdk class.
     *
     * By default the SDK will look for AWS credentials in your environment variables.
     * You can pass on your AWS credentials to the constructor too.
     * You must specify the region your SQS queues are setup.
     *
     * @param string $key AWS key
     * @param string $secret AWS secret
     * @param string $region AWS region
     * @param string $debug Debug flag
     *
     * @return void
     */
    public function __construct($key = false, $secret = false, $region = false, $debug = false)
    {
        if (!$region &&
            !($region = getenv("AWS_DEFAULT_REGION")))
            throw new \Exception("Set 'AWS_DEFAULT_REGION' environment variable!");
        
        $this->region = $region;
        $this->debug  = $debug;

        // Create AWS SDK instance
        $this->sts = new \Aws\Sts\StsClient([
                'region'  => $region,
                'version' => 'latest'
            ]);
        $this->sqs = new \Aws\Sqs\SqsClient([
                'region' => $region,
                'version' => 'latest'
            ]);
    }
    
    /**
     * Poll for incoming SQS messages using this method.
     *
     * Call this method in a loop in your client application.
     * If a new message is polled, it is returned to the caller.
     *
     * @param string|object $client JSON string or PHP Object created using json_decode. This contains your client SQS configuration which list your application `input` and `output` queues. 
     * @param integer $timeout Timeout value for the polling. After $timeout, the poller will return.
     *
     * @return object|false The SQS message or false if nothing. Can throw an Exception. Use try catch.
     */
    public function receive_message($client, $timeout)
    {
        $decodedClient = is_object($client) ? $client : json_decode($client);

        $queue = $decodedClient->{'queues'}->{'output'};
        if ($this->debug)
            $this->log_out(
                "DEBUG", 
                "Polling from '$queue' ..."
            );
        
        // Poll from SQS to check for new message 
        $result = $this->sqs->receiveMessage(array(
                'QueueUrl'        => $queue,
                'WaitTimeSeconds' => $timeout,
            ));
            
        // Get the message if any and return it to the caller
        if (($messages = $result->get('Messages')) &&
            count($messages))
        {
            if ($this->debug)
                $this->log_out(
                    "DEBUG", 
                    "New messages recieved in queue: '$queue'"
                );
            
            return $messages[0];
        }
        
        return false;
    }
    
    /**
     * Delete the provided message from the SQS queue
     *
     * Call this method from your client application after receiving a message
     * You must clean the SQS yourself, or you will keep processing the same messages
     *
     * @param string|object $client JSON string or PHP Object created using json_decode. This contains your client SQS configuration which list your application `input` and `output` queues. 
     * @param object $msg The message oject you received from `receive_message``
     *
     * @return true Can throw an Exeception if it fails. Use try catch.
     */
    public function delete_message($client, $msg)
    {
        $decodedClient = is_object($client) ? $client : json_decode($client);
        $this->sqs->deleteMessage(array(
                'QueueUrl'        => $decodedClient->{'queues'}->{'output'},
                'ReceiptHandle'   => $msg['ReceiptHandle']));
        
        return true;
    }
    
    /**
     * Send a `start_job` command to CPE
     *
     * Call this method from your client application to start a new job.
     * You can pass an object or a JSON string as input payload.
     * You can also specify your own jobId, or it will generate it for you.
     *
     * @param string|object $client JSON string or PHP Object created using json_decode. This contains your client SQS configuration which list your application `input` and `output` queues. 
     * @param object|string $input The data payload to be sent out to CPE. This will be used 
     *    as input for your workflow.
     * @param string|null $jobId JobID for your new job. Will be generated for you if not provided.
     *
     * @return string Return the JobIb for the job. Can throw execeptions if it fails. Use try catch.
     */
    public function start_job($client, $input, $jobId = null)
    {
        $decodedClient = is_object($client) ? $client : json_decode($client);
        $decodedInput = is_object($input) ? $input : json_decode($input);

        if (!$decodedClient) {
            throw new \InvalidArgumentException(
                "Invalid JSON 'client' to start new job!");
        }
        if (!$decodedInput) {
            throw new \InvalidArgumentException(
                "Invalid JSON 'input' to start new job!");
        }

        $this->validate_client($decodedClient);

        $jobId = $jobId ?: md5($decodedClient->{'name'} . uniqid('',true));
        $msg = $this->craft_new_msg(
            self::START_JOB,
            $jobId,
            $decodedClient,
            $decodedInput
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $decodedClient->{'queues'}->{'input'},
                'MessageBody' => json_encode($msg),
            ));

        return ($jobId);
    }

    /**
     * FIXME: Need to implement other methods
     *
     * We can implement more methods such as:
     *    - cancel_job()
     *    - cancel_activity()
     */
    
    /** 
     * Simple logout function 
     * 
     * @param string $type Type of log message (error, debug, etc)
     * @param string $message Message to be printed out
     */
    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }
    
    /** 
     * Craft the object to be sent out to SQS 
     *
     * @param string $type Type of CPE command you send 
     * @param string $jobId Job ID for the command
     * @param string $data Data payload to be sent out
     */
    private function craft_new_msg($type, $jobId, $client, $data)
    {
        // Add client info to data
        $data->{'client'} = $client;
        // Build msg to send out
        $msg = array(
            'time'   => microtime(true),
            'type'   => $type,
            'jobId'  => $jobId,
            'data'   => $data
        );

        return $msg;
    }
    
    /** 
     * Validate Client object structure 
     * 
     * @param string|object $client JSON string or PHP Object created using json_decode. This contains your client SQS configuration which list your application `input` and `output` queues. 
     */
    private function validate_client($client)
    {
        if (!isset($client->{"name"}))
            throw new \Exception("'client' has no 'name'!");
        if (!isset($client->{"queues"}))
            throw new \Exception("'client' has no 'queues'!");
        if (!isset($client->{"queues"}->{'input'}))
            throw new \Exception("'client' has no 'input' queue!");
        if (!isset($client->{"queues"}->{'output'}))
            throw new \Exception("'client' has no 'output' queue !");
    }
}
