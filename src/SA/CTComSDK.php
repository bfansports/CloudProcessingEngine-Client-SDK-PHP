<?php

namespace SA;

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

    // Msg Types
    const JOB_STARTED        = "JOB_STARTED";
    const ACTIVITY_SCHEDULED = "ACTIVITY_SCHEDULED";

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
    
    // Print on STDOUT
    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }

    // Initialize SQS client
    // Get temporary credentials using AWS STS service
    private function init_sqs_client($client)
    {
        // If no client provided, we don't use tmp credentials
        // We use the credentials provided in controler
        if (!$client) {
            if (!$this->sqs) 
            {
                if ($this->debug)
                    $this->log_out("DEBUG", "Using local AWS credentials for SQS");
                $this->sqs = $this->aws->get('Sqs');
            }             
            return;
        }

        // Test if we need to renew tmp credentials
        if (isset($this->assumedRole) && $this->assumedRole &&
            isset($this->assumedRole["Credentials"]["Expiration"]) &&
            $this->assumedRole["Credentials"]["Expiration"])
        {
            $time = strtotime($this->assumedRole["Credentials"]["Expiration"]);
            
            if ($time - time() > 300) {
                if ($this->debug)
                    $this->log_out("DEBUG", "Credentials still valid!");
                return;
            }
        }
         
        // Get new TMP credentials using AWS STS service
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
            $this->log_out("DEBUG", "Getting new temporary credentials from SQS.");

        // Get TMP credentials
        $this->assumedRole = $this->sts->assumeRole($assume);
        
        $this->credentials = 
            $this->sts->createCredentials($this->assumedRole);

        if ($this->debug)
            $this->log_out("DEBUG", "Creating SQS client using temporary credentials!");
            
        // Create SQS Client using new credentials
        $this->sqs = Sqs\SqsClient::factory(array(
                'credentials' => $this->credentials,
                'region'      => $this->region
            ));
        
        return;
    }
    
    /**
     * RECEIVE
     */

    // Poll one message at a time from the provided SQS queue
    public function receive_message($client, $queue, $timeout)
    {
        // Init SQS client
        $this->init_sqs_client($client);
 
        if ($this->debug)
            $this->log_out(
                "INFO", 
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
                    "INFO", 
                    "New messages recieved in queue: '$queue'"
                );
            
            return $messages[0];
        }
    }
    
    // Delete a message from SQS queue
    public function delete_message($client, $queue, $msg)
    {
        // Init SQS client
        $this->init_sqs_client($client);
        
        $this->sqs->deleteMessage(array(
                'QueueUrl'        => $queue,
                'ReceiptHandle'   => $msg['ReceiptHandle']));
        
        return true;
    }


    /**
     * SEND FROM CloudTranscode
     */

    public function job_queued()
    {
    }

    public function job_scheduled()
    {
    }

    public function job_started($workflowExecution, $workflowInput)
    {
        $decoded = $this->validate_workflow_input($workflowInput);

        // Init SQS client
        $this->init_sqs_client(false);

        $job_id = $decoded->{"job_id"};
        $client = $decoded->{"client"};
        
        $msg = $this->craft_new_msg(
            self::JOB_STARTED,
            array(
                'job_id'     => $decoded->{'job_id'},
                'runId'      => $workflowExecution['runId'],
                'workflowId' => $workflowExecution['workflowId'],
            )
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $client->{'queues'}->{'output'},
                'MessageBody' => json_encode($msg),
            ));
    }

    public function job_completed()
    {
        
    }

    public function job_failed()
    {
    }

    public function job_timeout()
    {
    }

    public function job_canceled()
    {
    }

    public function job_terminated()
    {
    }

    public function activity_scheduled($workflowExecution, $workflowInput, $activity)
    {
        $decoded = $this->validate_workflow_input($workflowInput);

        // Init SQS client
        $this->init_sqs_client(false);

        $job_id = $decoded->{"job_id"};
        $client = $decoded->{"client"};
        
        $msg = $this->craft_new_msg(
            self::ACTIVITY_SCHEDULED,
            array(
                'job_id'     => $decoded->{'job_id'},
                'runId'      => $workflowExecution['runId'],
                'workflowId' => $workflowExecution['workflowId'],
                'activity'   => $activity
            )
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $client->{'queues'}->{'output'},
                'MessageBody' => json_encode($msg),
            ));
    }

    public function activity_started()
    {
    }

    public function activity_completed()
    {
    }

    public function activity_failed()
    {
    }

    public function activity_timeout()
    {
    }

    public function activity_canceled()
    {
    }

    public function activity_progress()
    {
    }

    /**
     * SEND FROM Client
     */

    public function start_job()
    {
        // Return Job ID
    }

    public function cancel_job()
    {
    }

    public function cancel_activity()
    {
    }

    public function get_job_list()
    {
    }

    public function get_job_status()
    {
    }

    public function get_activity_status()
    {
    }

    /**
     * UTILS
     */

    private function craft_new_msg($type, $body)
    {
        $msg = array(
            'time' => time(),
            'type' => $type,
            'body' => $body
        );

        return $msg;
    }

    private function validate_workflow_input($input)
    {
        if (!($decoded = json_decode($input)))
            throw new \Exception("Workflow JSON input invalid!");
        
        if (!isset($decoded->{"client"}))
            throw new \Exception("No 'client' provided in job input!");
        if (!isset($decoded->{"job_id"}))
            throw new \Exception( "No 'job_id' provided in job input!");
        
        return ($decoded);
    }
}
