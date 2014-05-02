<?php

require __DIR__ . "/../vendor/autoload.php";

// Amazon libraries
use Aws\Common\Aws;
use Aws\Swf\Exception;

class CTComSDK
{
    private $aws;
    private $sqs;

    function __construct($key = false, $secret = false, $region = false)
    {
        if (!$key)
            if (!($key = getenv("AWS_ACCESS_KEY_ID")))
                throw new Exception("Provide AWS 'key'!");
        if (!$secret)
            if (!($secret = getenv("AWS_SECRET_KEY")))
                throw new Exception("Provide AWS 'secret'!");
        if (!$region)
            if (!($region = getenv("AWS_REGION")))
                throw new Exception("Provide AWS 'region'!");
        
        // Create AWS SDK instance
        $this->aws = Aws::factory(array(
                'key'    => $key,
                'secret' => $secret,
                'region' => $region
            ));
        
        // SQS Client
        $this->sqs = $this->aws->get('Sqs');
    }

    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }
    
    public function receive_message($queue, $timeout)
    {
        // Check the queue provided
        // If not existing, we create it!
        try {
            $queueUrl = $this->sqs->getQueueUrl(['QueueName' => $queue])['QueueUrl'];
        } catch (\Aws\Sqs\Exception\SqsException $e) {
            $this->log_out(
                "WARNING", 
                "Can't get SQS queue '$queue' ! Creating queue '$queue' ..."
            );
            
            try {
                $newQueue = $this->sqs->createQueue(['QueueName' => $queue]);
                $queueUrl = $newQueue['QueueUrl'];
            } catch (\Aws\Sqs\Exception\SqsException $e) {
                $this->log_out(
                    "ERROR", 
                    "Unable to create the new queue '$queue' ..."
                );
                return false;
            }
        }

        // Loop for message 
        $result = $this->sqs->receiveMessage(array(
                'QueueUrl'        => $queueUrl,
                'WaitTimeSeconds' => $timeout,
            ));

        if ($messages = $result->get('Messages'))
            return $messages;
    }

    public function delete_message($queue, $msg)
    {
        try {
            $queueUrl = $this->sqs->getQueueUrl(['QueueName' => $queue])['QueueUrl'];
        } catch (\Aws\Sqs\Exception\SqsException $e) {
            $this->log_out(
                "ERROR", 
                "Can't delete message from an invalid queue '$queue'.\n"
                . $e->getMessage()
            );
            return false;
        }

        $this->sqs->deleteMessage(array(
                'QueueUrl'        => $queueUrl,
                'ReceiptHandle'   => $msg['ReceiptHandle']));

        return true;
    }
}