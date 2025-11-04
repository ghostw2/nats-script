<?php
require_once __DIR__ . '/vendor/autoload.php';
use Basis\Nats\Client;
use Basis\Nats\Configuration;

const CORBOS_URL ="dev.autentica";
$configuration = new Configuration(
    ['host'=>'localhost',
    'port'=>4222,
    'password'=>'password']
);
$client = new Client($configuration);
$client->connect();
try{
    $accountInfo = $client->getApi()->getInfo();
    var_dump(accountInfo);

    //get my stram 
    $stream = $client->getApi()->getStream("stream_name");

    $streamInfo = $stream->getInfo();

    echo "\n Stream:".$streamInfo->config->name."\n";
    echo "\n Total images:".$streamInfo->state->messages."\n\n";

    $consumerConfig = new ConsumerConfiguration([
        "deliver_policy"=>'all',
        'ask_policy'=>'explicit',
        'replay_policy'=>'instant'
    ]);
    $consumer = $stream->createConsumer($consumerConfig);

    echo "Fetching all messages... \n\n";
    $allMessages = [];
    $totalCount = 0;
    while( true ){
        $messages = $consumer->fetch(100,2);
        if(empty($messages)){
            echo "no more messages";
            break;
        }
       foreach ($messages as $msg){
        $totalCount ++;
        $messageData = [
            'subject'=>$msg->subject,
            'payload'=>$msg->payload,
            'sequence'=>$msg->info() ? $msg->info()->streamSequence:null,
            'timestamp'=>$msg->info() ? date('Y-m-d H:i:s',$msg->info()->timestampNanos/1000000000):null,
        ];
        var_dump($messageData);
       }
    }


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


