from sftp import Sftp
import json
import requests
import base64
import os
#import ocrmypdf

def connect_to_sftp(hostname,username,password):
    sftp = Sftp(
            hostname=hostname,
            username=username,
            password=password,
            )
    return sftp

#def add_ocr_layer(local_dir_path, local_file_path):
   # ocr_file_path = local_dir_path + "output.pdf"
    #ocrmypdf.ocr(local_file_path, ocr_file_path, deskew=True)
    #if not os.path.exists(ocr_file_path):
        #os.rename(ocr_file_path, local_file_path)


def read_file_content(local_dir_path, filename):
    filepath = local_dir_path + filename
    file = open(filepath, 'rb')
    file_content = file.read()
    return file_content

def get_file_size(local_dir_path, filename):
    filepath = local_dir_path + filename
    file_stats = os.stat(filepath)
    return file_stats.st_size

def create_doc_corebos(corebos_info, data, file_content, file_size, logger):
    try: 
        corebos_url = corebos_info['corebos_url']
        corebos_token = corebos_info['corebos_authorization']
        

        filename = data['document_uuid']
        black_box_id = data['black_box_id']
        user_session_id = data['user_session_id']

        images_hashes = data['images_hashes']
        document_hash = data['document_hash']
        unix_time = data['unix_time']


        bytes_obj = bytes(file_content)
        element = json.dumps({
            "assigned_user_id": "19x1", 
            "notes_title": filename, 
            "filename":{ 
                "name":  filename,
                "size": str(file_size), 
                "type": "application/pdf", 
                "content": base64.b64encode(bytes_obj).decode('utf-8')
            },
            "black_box_id": black_box_id,
            "document_uuid": filename,
            "user_session_id": str(user_session_id),
            "images_hashes": str(images_hashes),
            "document_hash": document_hash,
            "unix_time": unix_time,
            "filetype": "application/pdf", 
            "filesize": str(file_size), 
            "filelocationtype": "I", 
            "filestatus": "1" 
        })

        response = requests.post(corebos_url,
        data = {"operation":"create", "elementType": "Documents", "element":element },            
        headers={"corebos-authorization": corebos_token})

        logger.info(f'Document {filename} was successfully created')

    except ConnectionError:
        logger.exception("Bad url")