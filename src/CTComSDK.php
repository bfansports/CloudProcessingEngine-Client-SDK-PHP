<?php

require __DIR__ . "/../vendor/autoload.php";

// Amazon libraries
use Aws\Common\Aws;
use Aws\Sqs;
use Aws\Sts;

class CTComSDK
{
    private $region;
    private $aws;
    private $sqs;
    private $sts;
    private $credentials;
    private $assumedRole;

    function __construct($key = false, $secret = false, $region = false)
    {
        /* if (!$key) */
        /*     if (!($key    = getenv("AWS_ACCESS_KEY_ID"))) */
        /*         throw new Exception("Provide AWS 'key'!"); */
        /* if (!$secret) */
        /*     if (!($secret = getenv("AWS_SECRET_KEY"))) */
        /*         throw new Exception("Provide AWS 'secret'!"); */
        /* if (!$region) */
        /*     if (!($region = getenv("AWS_REGION"))) */
        /*         throw new Exception("Provide AWS 'region'!"); */
        
        $this->region = $region;

        // Create AWS SDK instance
        $this->aws = Aws::factory(array(
                'key'    => $key,
                'secret' => $secret,
                'region' => $region
            ));
        $this->sts = $this->aws->get('Sts');
    }

    private function init_sqs_client($client)
    {
        if (isset($this->assumedRole) && $this->assumedRole)
        {
            print_r($this->assumedRole);
        }

        try {
            $role       = $client["SQS"]["role"];
            $externalId = $client["SQS"]["externalId"];
            $assume = array(
                'RoleArn'         => $role,
                'RoleSessionName' => time()."-".$client["client"],
                'DurationSeconds' => 900
            );
            if ($externalId && $externalId != "")
                $assume['ExternalId'] = $externalId;
            
            // Get TMP credentials
            $this->assumedRole = $this->sts->assumeRole($assume);
        } catch (\Aws\Sts\Exception\StsException $e) {
            $this->log_out("ERROR", $e->getMessage());
            return false;
        }
        
        try {
            $this->credentials = 
                $this->sts->createCredentials($this->assumedRole);

            // SQS Client
            $this->sqs = Sqs\SqsClient::factory(array(
                    'credentials' => $this->credentials,
                    'region'      => $this->region
                ));
        } catch (\Aws\Sqs\Exception\SqsException $e) {
            $this->log_out("ERROR", $e->getMessage());
            return false;
        }
        
        return true;
    }

    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }
    
    public function receive_message($client, $timeout)
    {
        if (!$this->init_sqs_client($client))
            return false;
 
        try {
            // Loop for message 
            $queue = $client["SQS"]["output"];
            $result = $this->sqs->receiveMessage(array(
                    'QueueUrl'        => $queue,
                    'WaitTimeSeconds' => $timeout,
                ));
        }
        catch (\Aws\Sqs\Exception\SqsException $e) {
            $this->log_out("ERROR", $e->getMessage());
            sleep($timeout);
            return false;
        }
        
        if ($messages = $result->get('Messages'))
            return $messages;
    }

    public function delete_message($client, $msg)
    {
        if (!$this->init_sqs_client($client))
            return false;
        
        try {
            $queue = $client["SQS"]["output"];
            $this->sqs->deleteMessage(array(
                    'QueueUrl'        => $queue,
                    'ReceiptHandle'   => $msg['ReceiptHandle']));
        } catch (\Aws\Sqs\Exception\SqsException $e) {
            $this->log_out("ERROR", $e->getMessage());
            return false;
        }
        
        return true;
    }
}