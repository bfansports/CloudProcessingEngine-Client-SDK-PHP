<?php

require __DIR__ . "/../vendor/autoload.php";

// Amazon libraries
use Aws\Common\Aws;
use Aws\Swf\Exception;

class CTComSDK
{
    private $aws;
    private $sqs;

    function __construct($key, $secret)
    {
        if (!$key && !$secret)
        {
            if (!getenv("AWS_ACCESS_KEY_ID") || !getenv("AWS_SECRET_KEY"))
                throw new Exception("Provide AWS 'key' and 'secret'!");
            else {
                $key    = getenv("AWS_ACCESS_KEY_ID");
                $secret = getenv("AWS_SECRET_KEY");
            }
        }
        
        // Create AWS SDK instance
        $this->aws = Aws::factory(array(
                'key'    => $key,
                'secret' => $secret
            ));
        
        // SQS Client
        $this->sqs = $this->aws->get('Sqs');
    }

    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }
    
    public function poll_incoming_msg($queue)
    {
        try {
            $queueUrl = $this->sqs->getQueueUrl(['QueueName' => $queue])['QueueUrl'];
        } catch (\Aws\Sqs\Exception\SqsException $e) {
            log_out(
                "ERROR", 
                "Can't get SQS queue '$queue' ! Creating queue '$queue' ..."
            );
            
            try {
                $newQueue = $sqs->createQueue(['QueueName' => $queue]);
                $queueUrl = $newQueue['QueueUrl'];
            } catch (\Aws\Sqs\Exception\SqsException $e) {
                log_out(
                    "ERROR", 
                    "Unable to create the new queue '$queue' ... skipping"
                );
                return false;
            }
        }

        while (42)
        {
            $result = $this->sqs->receiveMessage(array(
                    'QueueUrl'        => $queueUrl,
                    'WaitTimeSeconds' => 5,
                ));

            $messages = $result->get('Messages');
            if ($messages && count($messages)) 
                return $messages;
        }
    }
}