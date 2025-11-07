<?php
require_once __DIR__ . '/vendor/autoload.php';
use Basis\Nats\Client;
use Basis\Nats\Configuration;
  use Dotenv\Dotenv;
  use Basis\Nats\Stream\RetentionPolicy;
use Basis\Nats\Stream\StorageBackend;
use Basis\Nats\Consumer\Configuration as ConsumerConfiguration;
use Basis\Nats\Consumer\DeliverPolicy;


$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();

$corbos_url = $_ENV['CORBOS_URL'];
$corbos_token = $_ENV['CORBOS_TOKEN'];

$nats_port = $_ENV['NATS_PORT'];
$nats_host = $_ENV['NATS_HOST'];
$nats_user = $_ENV['NATS_USER'];
$nats_password = $_ENV['NATS_PASSWORD'];
$stream_name = $_ENV["STREAM_NAME"];

$hours_amount = $_ENV['HOURS_AMOUNT'];
$files_location = $_ENV['UPLOADED_FILE_LOCATION'];

echo $nats_host;
$configuration = new Configuration(
    [
        'host'=> $nats_host,
        'port'=>(int)$nats_port,
        //'user'=>$nats_user,
        //'pass'=>$nats_password  uncomment these in production
    ]
);
$configuration->setDelay(0.001);
$client = new Client($configuration);
$client->ping(); // true
try{
$stream = $client->getApi()->getStream($stream_name);
    
    // Get stream info
    $streamInfo = $stream->info();
    $messageCount = $streamInfo->state->messages;
    
    echo "Stream has {$messageCount} messages\n\n";
    // Create a new ephemeral consumer that starts from the beginning
    $consumerName = 'replayer_' . time();
    $consumerConfig = new ConsumerConfiguration($stream_name, $consumerName);

    $consumerConfig->setDeliverPolicy(DeliverPolicy::BY_START_TIME);
    
    $dateTimeOneHourAgo = new DateTime(); // Creates a DateTime object for the current time
    $dateTimeOneHourAgo->modify('-'.$hours_amount.' hour');
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
       
        $json_body =json_decode($message->body,true);
        if (json_last_error() !== JSON_ERROR_NONE){
            throw new Exception("JSON ERROR :" .json_last_error_msg);
            return;
        }       
         echo "Message {$messagesReceived}:\n";
        echo "  Subject: " . $message->subject . "\n";
        echo "  Body: " . $message->body . "\n";
        echo "  orderId:".$json_body['orderId']."\n";
        echo "\n";
        // Acknowledge the message
       // $message->ack();
    });
    
    echo "Done! Received {$messagesReceived} messages.\n";

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
function resend_file_to_webservice($data){
    
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
    $file_size = mb_strlen($bytes_file);

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
    
}
function generate_document_destination($datetime){
    $year = $datetime->format('Y');
    $month = $datetime->format('F');
    $firstDayOfMonth = new DateTime($date->format('Y-m-01'));
    $currentDay = (int)$datetime->format('j');
    
    // Get the day of week for the first day (0=Sunday, 6=Saturday)
    $firstDayOfWeek = (int)$firstDayOfMonth->format('w');
    
    // Calculate week number
    $weekOfMonth = ceil(($currentDay + $firstDayOfWeek) / 7);
    return "storage/$year/$month/week$weekOfMonth/";
}
function get_corbos_challange(){
    $ch = curl_init($corbosUrl."/webservice.php?operation=getchallenge&username=admin");
    curl_setopt($ch,CURLOPT_GET,1);
    // curl_setopt($ch,CURLOPT_POSTFIELDS,http_buld_query($post_data));
    // curl_setopt($ch,CURLOPT_HTTPHEADER,[
    //     'corebos-authorization: '. $corebos_token
    // ]);
    curl_setopt($ch,CURLOPT_RETURNTRANSFER, true);
    $response = curl_exec($ch);
    if (curl_errno($ch)){
        echo "cURL Error : ".curl_error($ch);
    }

}
function loginCorbos(){
    
}
?>


