import boto3
import math
import random
import json
import os
import getopt

#create a MANIFEST file and upload to S3
def create_manifest(bucket, prefix, entries):
    rand = math.floor(int(random.random()) * 10000)
    manifest_name = "manifest-%s.manifest" % (rand)

    manifest_file = {'entries':[]}
    for entri in entries:
        manifest_file['entries'].append({
            'url': 's3://%s/%s' % (bucket,entri),
            'mandatory': True
        })

    #print("MANIFEST FILE ELEMENTS::  " + len(manifest_file))

    #salvando o arquivo na pasta temporaria
    _file = "/tmp/%s" % (manifest_name)
    with open(_file, 'w') as out:
        json.dump(manifest_file, out)

    if os.path.isfile(_file):
        key = prefix + '/' + manifest_name

        #fazendo o upload do arquivo no diretorio especificado
        s3_client = boto3.client('s3')
        s3_client.upload_file(_file,bucket,key)
    else:
        raise Exception("Arquivo noo salvo")

    return key


#create a entries collection
def retrieve_entries(bucket, prefix, file_extension):
    entries = []
    #retrieve the files in bucket
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    #response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    options = {'Bucket':bucket, 'Prefix':prefix}
    page_iterator = paginator.paginate(**options)
    for page in page_iterator:
        for resp in page['Contents']:
            if (not resp['Key'].startswith('$')) and (resp['Key'].endswith(file_extension)):
                entries.append(resp['Key'])
    return entries


def main(argv):

    supported_args = """bucket_in= prefix_in= bucket_out= prefix_out= file_extension="""

    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        raise Exception(err)

    for arg, value in optlist:
        if arg == "--bucket_in":
            if value is None:
                raise Exception("You need to inform all valid parameters")
            s3_in_bucket = value
        if arg == "--bucket_out":
            if value is None:
                raise Exception("You need to inform all valid parameters")
            s3_bucket_out = value
        if arg == "--prefix_in":
            if value is None:
                raise Exception("You need to inform all valid parameters")
            s3_in_prefix = value
        if arg == "--prefix_out":
            if value is None:
                raise Exception("You need to inform all valid parameters")
            s3_out_prefix = value
        if arg == "--file_extension":
            if value is None:
                file_extension = "gz"
            else:
                file_extension = value

    entries = retrieve_entries(bucket_in, s3_in_prefix, file_extension)
    print("Entries size: {}".format(len(entries)))
    if len(entries) > 0:
        create_manifest(bucket_out, s3_out_prefix, entries)
    else:
        print("No files encountered to create a MANIFEST")

if __name__ == '__main__':
    main(argv)
