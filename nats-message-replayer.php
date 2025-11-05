<?php
require_once __DIR__ . '/vendor/autoload.php';
use Basis\Nats\Client;
use Basis\Nats\Configuration;
  use Dotenv\Dotenv;
  use Basis\Nats\Stream\RetentionPolicy;
use Basis\Nats\Stream\StorageBackend;
use Basis\Nats\Consumer\Configuration as ConsumerConfiguration;
use Basis\Nats\Consumer\DeliverPolicy;


const CORBOS_URL ="dev.autentica";
 $dotenv = Dotenv::createImmutable(__DIR__);
 $dotenv->load();
$configuration = new Configuration(
    ['host'=>'localhost',
    'port'=>4222,
    ]
);
$configuration->setDelay(0.001);
$client = new Client($configuration);
$client->ping(); // true
try{
$stream = $client->getApi()->getStream($_ENV['STREAM_NAME']);
    
    // Get stream info
    $streamInfo = $stream->info();
    $messageCount = $streamInfo->state->messages;
    
    echo "Stream has {$messageCount} messages\n\n";
    // Create a new ephemeral consumer that starts from the beginning
    $consumerName = 'replayer_' . time();
    $consumerConfig = new ConsumerConfiguration($_ENV['STREAM_NAME'], $consumerName);
     echo "ConsumerConfiguration methods:\n";
    $methods = get_class_methods($consumerConfig);
    //var_dump($methods);
    $consumerConfig->setDeliverPolicy(DeliverPolicy::BY_START_TIME);
    
    $dateTimeOneHourAgo = new DateTime(); // Creates a DateTime object for the current time
    $dateTimeOneHourAgo->modify('-1 hour');
    $consumerConfig->setStartTime($dateTimeOneHourAgo);
    // functional $consumerConfig->setDeliverPolicy(DeliverPolicy::ALL);
    // $consumerConfig->setAckWait(30);
    

    // Create consumer by calling getConsumer - it will create if doesn't exist
    $consumer = new \Basis\Nats\Consumer\Consumer($client, $consumerConfig);
    $consumer->create();
    
    // Set iterations
    $consumer->setIterations($messageCount);
   
    
    echo "Fetching messages using 'greeter' consumer...\n\n";
    
    // Handle messages
    $messagesReceived = 0;
    $consumer->handle(function($message) use (&$messagesReceived) {
        $messagesReceived++;
        echo "Message {$messagesReceived}:\n";
        echo "  Subject: " . $message->subject . "\n";
        echo "  Body: " . $message->body . "\n";
        echo "\n";
        
        // Acknowledge the message
       // $message->ack();
    });
    
    echo "Done! Received {$messagesReceived} messages.\n";

    // echo "Fetching all messages... \n\n";
    // $allMessages = [];
    // $totalCount = 0;
    // while( true ){
    //     $messages = $consumer->fetch(100,2);
    //     if(empty($messages)){
    //         echo "no more messages";
    //         break;
    //     }
    //    foreach ($messages as $msg){
    //     $totalCount ++;
    //     $messageData = [
    //         'subject'=>$msg->subject,
    //         'payload'=>$msg->payload,
    //         'sequence'=>$msg->info() ? $msg->info()->streamSequence:null,
    //         'timestamp'=>$msg->info() ? date('Y-m-d H:i:s',$msg->info()->timestampNanos/1000000000):null,
    //     ];
    //     var_dump($messageData);
    //    }
    // }


}
catch (Exception $e ){
    echo "Error :".$e->getMessage()."\n";
}finally{
    $client->disconnect();
}

function check_file_exists($filename,$corBosPath){
    if(!file_exists($corBosPath.DIRECTORY_SEPARATOR.$filename)){
        return false;
    }  else {
        return true;
    }
}
function resend_file_to_webservice($data,$file){
    
    $data = decodeMessageBody(data);
    $doc_uuid = data['document_uuid'];
    echo "document uuid:".doc_uuid;

    $corbosUrl = CORBOS_URL;
    $corbosToken = "token";
    
    $filename = $data["document_uuid"];
    $black_box_id = $data["black_box_id"];
    $user_session_id = $data['user_session_id'];

    $images_hashes = $data['images_hashes'];
    $document_hash = $data['document_hash'];
    $unix_time = $data['unix_time'];
   
    $bytes_file =  file_get_contents($filename);
    $file_size = 1020;

    $json_element = json_encode(
        [
        "assigned_user_id"=> "19x1", 
            "notes_title"=> $filename, 
            "filename"=>[ 
                "name"=>  $filename,
                "size"=> (string)$file_size, 
                "type"=> "application/pdf", 
                "content"=> base64_encode(bytes_obj)
            ],
            "black_box_id"=> $black_box_id,
            "document_uuid"=> $filename,
            "user_session_id"=> (string)$user_session_id,
            "images_hashes"=> (string)$images_hashes,
            "document_hash"=> $document_hash,
            "unix_time"=> $unix_time,
            "filetype"=> "application/pdf", 
            "filesize"=> (string)$file_size, 
            "filelocationtype"=> "I", 
            "filestatus"=> "1" 
        ],JSON_FORCE_OBJECT);
    if ($element === false){
        echo "JSON encodin error ".json_last_error_msg();
    }
    $post_data = [
        "operation"=>"create",
        "elementType"=>"Documents",
        "element"=>$json_element
    ];
    
    $ch = curl_init($corbosUrl);
    curl_setopt($ch,CURLOPT_POST,1);
    curl_setopt($ch,CURLOPT_POSTFIELDS,http_buld_query($post_data));
    curl_setopt($ch,CURLOPT_HTTPHEADER,[
        'corebos-authorization: '. $corebos_token
    ]);
    curl_setopt($ch,CURLOPT_RETURNTRANSFER, true);
    $response = curl_exec($ch);
    if (curl_errno($ch)){
        echo "cURL Error : ".curl_error($ch);
    }
    

    //add the metadata form the server 
    //add the login 
    //recreate the json contents
    //send the request to corbos webservice
}
function decodeMessageBody($string){
    return "";
}

?>


