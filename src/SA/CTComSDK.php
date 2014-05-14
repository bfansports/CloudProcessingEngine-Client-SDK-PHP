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
    const START_JOB          = "START_JOB";
    const JOB_STARTED        = "JOB_STARTED";
    const ACTIVITY_SCHEDULED = "ACTIVITY_SCHEDULED";
    const ACTIVITY_STARTED   = "ACTIVITY_STARTED";
    const ACTIVITY_FAILED    = "ACTIVITY_FAILED";
    const ACTIVITY_TIMEOUT   = "ACTIVITY_TIMEOUT";
    const ACTIVITY_COMPLETED = "ACTIVITY_COMPLETED";
    const ACTIVITY_PROGRESS  = "ACTIVITY_PROGRESS";
    const ACTIVITY_PREPARING = "ACTIVITY_PREPARING";
    const ACTIVITY_FINISHING = "ACTIVITY_FINISHING";

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
         
        // Get new TMP credentials for client using AWS STS service
        $role       = $client->{"role"};
        $externalId = $client->{"externalId"};
        $assume = array(
            'RoleArn'         => $role,
            'RoleSessionName' => time()."-".$client->{"name"},
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
        $this->validate_workflow_input($workflowInput);

        // Init SQS client
        $this->init_sqs_client(false);

        $job_id = $workflowInput->{"job_id"};
        $client = $workflowInput->{"client"};
        
        $msg = $this->craft_new_msg(
            self::JOB_STARTED,
            $workflowInput->{'job_id'},
            array(
                'workflow' => $workflowExecution
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
        $this->send_activity_status(
            $workflowExecution, 
            $workflowInput, 
            $activity, 
            self::ACTIVITY_SCHEDULED
        );
    }

    public function activity_started($workflowExecution, $workflowInput, $activity)
    {
        $this->send_activity_status(
            $workflowExecution, 
            $workflowInput, 
            $activity, 
            self::ACTIVITY_STARTED
        );
    }

    public function activity_completed($workflowExecution, $workflowInput, $activity)
    {
        $this->send_activity_status(
            $workflowExecution, 
            $workflowInput, 
            $activity, 
            self::ACTIVITY_COMPLETED
        );
    }

    public function activity_failed($workflowExecution, $workflowInput, $activity)
    {
        $this->send_activity_status(
            $workflowExecution, 
            $workflowInput, 
            $activity, 
            self::ACTIVITY_FAILED
        );
    }

    public function activity_timeout($workflowExecution, $workflowInput, $activity)
    {
        $this->send_activity_status(
            $workflowExecution, 
            $workflowInput, 
            $activity, 
            self::ACTIVITY_TIMEOUT
        );
    }

    public function activity_canceled()
    {
    }

    public function activity_progress($task, $progress)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_PROGRESS, 
            $progress
        );
    }

    public function activity_preparing($task)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_PREPARING, 
            false
        );
    }

    public function activity_finishing($task)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_FINISHING, 
            false
        );
    }

    /**
     * SEND FROM Client
     */

    public function start_job($client, $input)
    {
        if (!$client)
            throw new \Exception("You must provide 'client' JSON identification!");
        if (!$input)
            throw new \Exception("You must provide a JSON 'input' to start a new job!");
        
        if (!($decodedInput  = json_decode($input)))
            throw new \Exception("Invalid JSON 'input' to start new job!");
        
        $this->validate_client($client);
        
        // Init SQS client
        $this->init_sqs_client($client);
        
        $job_id = md5($client->{'name'} . uniqid('',true));
        $msg = $this->craft_new_msg(
            self::START_JOB,
            $job_id,
            $decodedInput
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $client->{'queues'}->{'input'},
                'MessageBody' => json_encode($msg),
            ));

        return ($job_id);
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

    private function send_activity_status(
        $workflowExecution, 
        $workflowInput, 
        $activity, 
        $type)
    {
        $this->validate_workflow_input($workflowInput);

        // Init SQS client
        $this->init_sqs_client(false);

        $job_id = $workflowInput->{"job_id"};
        $client = $workflowInput->{"client"};
        
        $msg = $this->craft_new_msg(
            $type,
            $job_id,
            array(
                'workflow' => $workflowExecution,
                'activity' => $activity
            )
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $client->{'queues'}->{'output'},
                'MessageBody' => json_encode($msg),
            ));
    }

    private function send_activity_updates($task, $type, $extra)
    {
        // Init SQS client
        $this->init_sqs_client(false);

        if (!($input = json_decode($task->get('input'))))
            throw new \Exception("Task input JSON is invalid!");
        $job_id = $input->{"job_id"};
        $client = $input->{"client"};
        
        $activity = [
            'activityId'   => $task->get('activityId'),
            'activityType' => $task->get('activityType')
        ];

        // Prepare data to be send out to client
        $data = array(
            'workflow' => $task->get('workflowExecution'),
            'activity' => $activity
        );
        
        // Add extra data to $data
        if ($extra && is_array($extra) && count($extra))
            foreach ($extra as $key => $value)
                $data[$key] = $value;
        
        $msg = $this->craft_new_msg(
            $type,
            $job_id,
            $data
        );

        $this->sqs->sendMessage(array(
                'QueueUrl'    => $client->{'queues'}->{'output'},
                'MessageBody' => json_encode($msg),
            ));
    }


    private function validate_workflow_input($input)
    {
        if (!isset($input->{"client"}))
            throw new \Exception("No 'client' provided in job input!");
        if (!isset($input->{"job_id"}))
            throw new \Exception("No 'job_id' provided in job input!");
        if (!isset($input->{"data"}))
            throw new \Exception("No 'data' provided in job input!");
    }
    
    private function validate_client($client)
    {
        if (!isset($client->{"name"}))
            throw new \Exception("'client' has no 'name'!");
        if (!isset($client->{"role"}))
            throw new \Exception("'client' has no 'role'!");
        if (!isset($client->{"queues"}))
            throw new \Exception("'client' has no 'queues'!");
        if (!isset($client->{"queues"}->{'input'}))
            throw new \Exception("'client' has no 'input' queue!");
        if (!isset($client->{"queues"}->{'output'}))
            throw new \Exception("'client' has no 'output' queue !");
    }
}
