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

    const ROLE_DURATION = 3600;

    function __construct($key = false, $secret = false, $region = false, $debug = false)
    {
        if (!$key)
            if (!($key    = getenv("AWS_ACCESS_KEY_ID")))
                throw new Exception("Provide AWS 'key'!");
        if (!$secret)
            if (!($secret = getenv("AWS_SECRET_KEY")))
                throw new Exception("Provide AWS 'secret'!");
        if (!$region)
            if (!($region = getenv("AWS_REGION")))
                throw new Exception("Provide AWS 'region'!");
        
        $this->region = $region;
        $this->debug  = $debug;

        // Create AWS SDK instance
        $this->aws = Aws::factory(array(
                'key'    => $key,
                'secret' => $secret,
                'region' => $region
            ));
        $this->sts = $this->aws->get('Sts');
    }
    
    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }

    private function init_sqs_client($client)
    {
        // Test if we need to renew credentials
        if (isset($this->assumedRole) && $this->assumedRole &&
            isset($this->assumedRole["Credentials"]["Expiration"]) &&
            $this->assumedRole["Credentials"]["Expiration"])
        {
            $time = strtotime($this->assumedRole["Credentials"]["Expiration"]);
            
            if ($time - time() > 300) {
                if ($this->debug)
                    $this->log_out("DEBUG", "Credentials still valid!");
                return true;
            }
        }
         
        // Get new TMP credentials using AWS STS service
        try {
            $role       = $client["role"];
            $externalId = $client["externalId"];
            $assume = array(
                'RoleArn'         => $role,
                'RoleSessionName' => time()."-".$client["name"],
                'DurationSeconds' => self::ROLE_DURATION
            );
            if ($externalId && $externalId != "")
                $assume['ExternalId'] = $externalId;
            
            if ($this->debug)
                $this->log_out("DEBUG", "Getting new credentials from STS service!");

            // Get TMP credentials
            $this->assumedRole = $this->sts->assumeRole($assume);
        } catch (\Aws\Sts\Exception\StsException $e) {
            $this->log_out("ERROR", $e->getMessage());
            return false;
        }
        
        try {
            $this->credentials = 
                $this->sts->createCredentials($this->assumedRole);

            if ($this->debug)
                $this->log_out("DEBUG", "Creating SQS client using temporary credentials!");
            
            // Create SQS Client using new credentials
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
    
    public function receive_message($client, $queue, $timeout)
    {
        if (!$this->init_sqs_client($client))
            return false;
 
        try {
            if ($this->debug)
                $this->log_out(
                    "INFO", 
                    "Polling from '$queue' ..."
                );
            
            // Loop for message 
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
        
        if (($messages = $result->get('Messages')) &&
            count($message))
        {
            if ($this->debug)
                $this->log_out(
                    "INFO", 
                    "New messages recieved in queue: '$queue'"
                );
            
            return $messages[0];
        }
    }
    
    public function delete_message($client, $queue, $msg)
    {
        if (!$this->init_sqs_client($client))
            return false;
        
        try {
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