<?php

namespace SA;

// Amazon libraries
use Aws\Common\Aws;
use Aws\Sqs;
use Aws\Sts;

class CpeClientSdk
{
    private $region;
    private $aws;
    private $sqs;
    private $sts;
    
    // Msg Types
    const START_JOB          = "START_JOB";
    // XXX Add missing calls
    // const CANCEL_JOB         = "CANCEL_JOB";

    function __construct(
        $key = false,
        $secret = false,
        $region = false,
        $debug = false
    )
    {
        if (!$key &&
            !($key = getenv("AWS_ACCESS_KEY_ID")))
            throw new \Exception("Set 'AWS_ACCESS_KEY_ID' environment variable!");
        if (!$secret &&
            !($secret = getenv("AWS_SECRET_KEY")))
            throw new \Exception("Set 'AWS_SECRET_KEY' environment variable!");
        if (!$region &&
            !($region = getenv("AWS_DEFAULT_REGION")))
            throw new \Exception("Set 'AWS_DEFAULT_REGION' environment variable!");
        
        $this->region = $region;
        $this->debug  = $debug;

        // Create AWS SDK instance
        $this->aws = Aws::factory(array(
                'region' => $region
            ));
        $this->sts = $this->aws->get('Sts');
        $this->sqs = $this->aws->get('Sqs');
    }
    
    // Poll one message at a time from the provided SQS queue
    // Queue
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
    }
    
    // Delete a message from SQS queue
    // To be called once the message has been pulled by the application
    public function delete_message($client, $msg)
    {
        $decodedClient = is_object($client) ? $client : json_decode($client);
        $this->sqs->deleteMessage(array(
                'QueueUrl'        => $decodedClient->{'queues'}->{'output'},
                'ReceiptHandle'   => $msg['ReceiptHandle']));
        
        return true;
    }

    // Send a new JOB to SQS Input queue
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
            $decodedInput
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $decodedClient->{'queues'}->{'input'},
                'MessageBody' => json_encode($msg),
            ));

        return ($jobId);
    }

    // XXX TODO
    /* public function cancel_job() */
    /* { */
    /* } */

    /* public function cancel_activity() */
    /* { */
    /* } */

    /* public function get_job_list() */
    /* { */
    /* } */

    /* public function get_job_status() */
    /* { */
    /* } */

    /* public function get_activity_status() */
    /* { */
    /* } */
    

    /**
     * UTILS
     */

    // Print on STDOUT
    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }

    // Craft a new SQS message
    private function craft_new_msg($type, $jobId, $data)
    {
        $msg = array(
            'time'   => microtime(true),
            'type'   => $type,
            'job_id' => $jobId,
            'data'   => $data
        );

        return $msg;
    }

    // Validate Client object
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
