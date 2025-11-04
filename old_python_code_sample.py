import asyncio
import os
from nats.aio.client import Client as NATS
from functools import partial
from dotenv import load_dotenv
from utilities import *
import logging
#import ocrmypdf

async def message_handler(msg, sftp_conn_cred, local_dir_path, remote_dir_path, corebos_info, logger):
     
    try:
        subject = msg.subject
        data = json.loads(msg.data.decode())
        doc_uuid = data['document_uuid']
        logger.info('Message arrived')

        sftp_server = sftp_conn_cred['sftp_server']
        sftp_username = sftp_conn_cred['sftp_username']
        sftp_password = sftp_conn_cred['sftp_password']
        
        corebos_url = corebos_info['corebos_url'] 
        local_file_path = local_dir_path + doc_uuid + ".pdf"
        remote_file_path = remote_dir_path + doc_uuid + ".pdf"

        try:
            logger.info("Connecting to sftp server")
            sftp = connect_to_sftp(sftp_server, sftp_username , sftp_password)
            sftp.connect()
            logger.info("Connected to sftp server")
            logger.info('Downloading file from sftp server')
            sftp.download(remote_file_path, local_file_path)
            logger.info(f'File downloaded and saved into {local_file_path}')
        
            filename = doc_uuid + '.pdf'
            #add_ocr_layer(local_dir_path, local_file_path)
            file_content = read_file_content(local_dir_path, filename)
            file_size = get_file_size(local_dir_path, filename)
        
            logger.info(f'Sending request to corebos {corebos_url}')
            create_doc_corebos(corebos_info, data, file_content, file_size, logger)

            sftp.disconnect()
            logger.info('Disconnected from sftp server')

        except Exception as err:
            logger.exception('Could not connect to sftp')
        
    
    except UnicodeError as ue:
        logger.exception('Message could not be decoded')

    except FileNotFoundError as fnfe:
        logger.exception('File in the remote server could not be found.')

async def subscribe(sftp_conn_cred, logger):

    nats_server = os.environ.get('nats_server')
    nats_user = os.environ.get('nats_user')
    nats_pass = os.environ.get('nats_pass')
    nats_stream = os.environ.get('nats_stream')
    nats_channel = os.environ.get('nats_channel')
    
    corebos_url = os.environ.get('corebos_url')
    corebos_authorization = os.environ.get('corebos_authorization')

    corebos_info = {'corebos_url': corebos_url,
                    'corebos_authorization': corebos_authorization}

    local_dir_path = os.environ.get('local_dir_path')
    remote_dir_path = os.environ.get('remote_dir_path')
    
    nats_url = "nats://" + nats_user + ":" + nats_pass + "@" + nats_server 

    try:
        logger.info('Connecting to nats-server')
        nc = NATS()
        await nc.connect(servers=[nats_url])
        logger.info('Connected to nats-server')

    except ConnectionError as cerr:
        logger.exception('Could not connect to nats server. Check if you have started the nats server.')
    
    except ModuleNotFoundError as mnferr:
        logger.exception('NATS module is not installed. Please installnats for python by using pip3 install nats-py')

    partial_callback = partial(message_handler, sftp_conn_cred = sftp_conn_cred, local_dir_path = local_dir_path, remote_dir_path = remote_dir_path, corebos_info = corebos_info, logger = logger)
    
    js = nc.jetstream()
    psub = await js.pull_subscribe("leonardo.leonardo_docs", "psub")
    while True:
        try:
            msgs = await psub.fetch(1, timeout=None)
            for msg in msgs:
                try:
                    await message_handler(msg, sftp_conn_cred, local_dir_path, remote_dir_path, corebos_info, logger)
                    await msg.ack()
                except Exception as e:
                    logger.exception(f'Error processing message: {e}')
        except Exception as e:
            logger.exception(f"Error fetching messages: {e}")
    

    logger.info(f'Subscribed to {nats_channel}. Waiting for messages.')


if __name__ == '__main__':
    
    load_dotenv()


    logger = logging.getLogger('processlogger')
    logger.setLevel(logging.INFO)
    file_handler_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('/home/nats/natssub/logs/app.log')
    file_handler.setFormatter(file_handler_format)

    logger.addHandler(file_handler)

    sftp_server = os.environ.get('sftp_server')
    sftp_username = os.environ.get('sftp_username')
    sftp_password = os.environ.get('sftp_password')
    
    sftp_conn_cred = {"sftp_server": sftp_server,
                    "sftp_username": sftp_username,
                    "sftp_password": sftp_password}

    loop = asyncio.new_event_loop()
    loop.run_until_complete(subscribe(sftp_conn_cred, logger))