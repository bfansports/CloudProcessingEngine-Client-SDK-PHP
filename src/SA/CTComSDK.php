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
    
    // Known Activities
    const VALIDATE_INPUT   = "ValidateInputAndAsset";
    const TRANSCODE_ASSET  = "TranscodeAsset";

    // Msg Types
    const START_JOB          = "START_JOB";
    const JOB_STARTED        = "JOB_STARTED";
    const JOB_COMPLETED      = "JOB_COMPLETED";
    const JOB_FAILED         = "JOB_FAILED";
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
        $this->sqs = $this->aws->get('Sqs');
    }
    
    // Print on STDOUT
    private function log_out($type, $message)
    {
        echo("[$type] $message\n");
    }
    
    /**
     * RECEIVE
     */

    // Poll one message at a time from the provided SQS queue
    public function receive_message($queue, $timeout)
    {
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
    public function delete_message($queue, $msg)
    {
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
        $this->send_job_updates(
            $workflowExecution,
            $workflowInput,
            self::JOB_STARTED,
            true
        );
    }

    public function job_completed($workflowExecution, $workflowInput)
    {
        $this->send_job_updates(
            $workflowExecution,
            $workflowInput,
            self::JOB_COMPLETED
        );
    }

    public function job_failed($workflowExecution, $workflowInput, $reason, $details)
    {
        $this->send_job_updates(
            $workflowExecution,
            $workflowInput,
            self::JOB_FAILED,
            false,
            [
                "reason"  => $reason,
                "details" => $details
            ]
        );
    }

    public function job_timeout()
    {
    }

    public function job_canceled()
    {
    }

    public function activity_started($task)
    {
        // last param to 'true' to force sending 'input' info back to client
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_STARTED,
            true
        );
    }

    public function activity_completed($task)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_COMPLETED
        );
    }

    public function activity_failed($task, $reason, $details)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_FAILED,
            false,
            [
                "reason"  => $reason,
                "details" => $details
            ]
        );
    }

    public function activity_timeout($workflowExecution, $workflowInput, $activity)
    {
        $this->validate_workflow_input($workflowInput);
        
        $job_id = $workflowInput->{"job_id"};
        $client = $workflowInput->{"client"};
        
        $activityData = [
            'activityId'   => $activity["activityId"],
            'activityType' => $activity["activityType"]
        ];
        
        $msg = $this->craft_new_msg(
            self::ACTIVITY_TIMEOUT,
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

    public function activity_canceled()
    {
    }

    public function activity_progress($task, $progress)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_PROGRESS, 
            false,
            $progress
        );
    }

    public function activity_preparing($task)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_PREPARING
        );
    }

    public function activity_finishing($task)
    {
        $this->send_activity_updates(
            $task, 
            self::ACTIVITY_FINISHING
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
        
        if (!($decodedInput = json_decode($input)))
            throw new \Exception("Invalid JSON 'input' to start new job!");
        
        $this->validate_client($client);
        
        $job_id = md5($client->{'name'} . uniqid('',true));
        $msg = $this->craft_new_msg(
            self::START_JOB,
            $job_id,
            $decodedInput
        );

        print("SEND MSG!\n");

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

    private function send_activity_updates(
        $task, 
        $type, 
        $sendInput = false, 
        $extra = false)
    {
        if (!($input = json_decode($task->get('input'))))
            throw new \Exception("Task input JSON is invalid!");
        $job_id = $input->{"job_id"};
        $client = $input->{"client"};
        
        $activityType = $task->get('activityType');
        $activity = [
            'activityId'   => $task->get('activityId'),
            'activityType' => $activityType
        ];
        
        // Include TASK input information
        // We know two task types. We format the 'input' object differently
        // based on the task type.
        if ($sendInput)
        {
            if ($activityType["name"] == self::VALIDATE_INPUT)
                $activity['input'] = $input->{"data"};
            else if ($activityType["name"] == self::TRANSCODE_ASSET)
            {
                $activity['input']['input_asset_type'] = $input->{"input_asset_type"};
                $activity['input']['input_asset_info'] = $input->{"input_asset_info"};
                $activity['input']['output'] = $input->{"output"};
            }
        }
        
        // Add extra data to $data
        if ($extra && is_array($extra) && count($extra))
            foreach ($extra as $key => $value)
                $activity[$key] = $value;
        
        // Prepare data to be send out to client
        $data = array(
            'workflow' => $task->get('workflowExecution'),
            'activity' => $activity
        );
        
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

    private function send_job_updates(
        $workflowExecution, 
        $workflowInput, 
        $type, 
        $sendInput = false, 
        $extra = false)
    {
        $this->validate_workflow_input($workflowInput);
        
        $job_id = $workflowInput->{"job_id"};
        $client = $workflowInput->{"client"};

        if ($sendInput)
            $workflowExecution['input'] = $workflowInput->{"data"};
        
        $data = array(
            'workflow' => $workflowExecution,
        );
        
        if ($extra && is_array($extra) && count($extra))
            foreach ($extra as $key => $value)
                $data[$key] = $value;
        
        $msg = $this->craft_new_msg(
            $type,
            $workflowInput->{'job_id'},
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
        if (!isset($client->{"queues"}))
            throw new \Exception("'client' has no 'queues'!");
        if (!isset($client->{"queues"}->{'input'}))
            throw new \Exception("'client' has no 'input' queue!");
        if (!isset($client->{"queues"}->{'output'}))
            throw new \Exception("'client' has no 'output' queue !");
    }
}
