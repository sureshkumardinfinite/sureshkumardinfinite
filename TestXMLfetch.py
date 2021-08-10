# get latest available xmls from source for the env
import boto3
from datetime import datetime

s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=ev_epg_s3_access_key_id,
    aws_secret_access_key=ev_epg_s3_secret_access_key,
)

prefix = jv_src_prefix.format(jv_env_name=jv_env_name, jv_env_type=jv_env_type)
bucket = jv_src_bucket
files = []
next_token = ''
base_kwargs = {
    'Bucket': bucket,
    'Prefix': prefix,
}

while next_token is not None:
    kwargs = base_kwargs.copy()
    if next_token != '':
        kwargs.update({'ContinuationToken': next_token})
    files_obj = s3_client.list_objects_v2(**kwargs)
    for item in files_obj['Contents']:
        if 'zip' in item['Key']:
            f_name = str.replace(item['Key'], prefix, '')
            files.append(
                {
                    'datetime_eq': datetime.strptime(
                        str.replace(f_name, '.zip', ''), '%d%m%Y%H%M'
                    ),
                    'name': f_name,
                }
            )
    next_token = files_obj.get('NextContinuationToken')

if files:
    files = sorted(files, key=lambda i: i['datetime_eq'])
    files_dt_eqs = [f['datetime_eq'] for f in files]
    files_names = [f['name'] for f in files]
    if jv_last_processed_file == '0.zip':
        file_to_process = files_names[0]
        if jv_new_file_exists == 0:
            context.updateVariable('jv_new_file_exists', 1)
        jv_envs_to_process = context.getGridVariable('jv_envs_to_process')
        if jv_envs_to_process:
            jv_envs_to_process += [
                [jv_env_name, str.replace(file_to_process, '.zip', '')]
            ]
            context.updateGridVariable('jv_envs_to_process', jv_envs_to_process)
        else:
            context.updateGridVariable(
                'jv_envs_to_process',
                [[jv_env_name, str.replace(file_to_process, '.zip', '')]],
            )
    else:
        try:
            file_to_process = files_names[files_names.index(jv_last_processed_file) + 1]
            if jv_new_file_exists == 0:
                context.updateVariable('jv_new_file_exists', 1)
            jv_envs_to_process = context.getGridVariable('jv_envs_to_process')
            if jv_envs_to_process:
                jv_envs_to_process += [
                    [jv_env_name, str.replace(file_to_process, '.zip', '')]
                ]
                context.updateGridVariable('jv_envs_to_process', jv_envs_to_process)
            else:
                context.updateGridVariable(
                    'jv_envs_to_process',
                    [[jv_env_name, str.replace(file_to_process, '.zip', '')]],
                )
        except:
            # no new file to process
            exit()
else:
    # directory empty
    exit()
