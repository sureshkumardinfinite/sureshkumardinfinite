# get xmls from source, filter what needs to be converted to s3
import io
import zipfile
import boto3
from xml.etree import ElementTree
from smart_open import open
from collections import OrderedDict

files_to_process = [
    'service.xml',
    'schedule.xml',
    'media.xml',
    'genre.xml',
    'program_media.xml',
    'media_type.xml',
    'region.xml',
    'region_service.xml',
    'service_media.xml',
    'sub_genre.xml',
]

file_definitions = {
    'service.xml': ['service_id', 'channel_name', 'channel_short_name', 'callsign'],
    'schedule.xml': [
        'event_id',
        'service_id',
        'event_date',
        'title_en',
        'title_ar',
        'movie',
        'series',
        'closed_captions',
        'classification',
        'classification_id',
        'warnings',
        'year_production',
        'program_id',
        'episode_title_en',
        'episode_title_ar',
        'preamble_title',
        'series_number',
        'episode_number',
        'episode_id',
        'genre_id',
        'sub_genre_id',
        'director_en',
        'director_ar',
        'main_cast_en',
        'main_cast_ar',
        'premiere',
        'repeat',
        'final',
        'series_return',
        'live',
        'high_definition',
        'widescreen',
        'colour',
        'subtitles',
        'audio',
        'country',
        'language',
        'synopsis_en',
        'synopsis_ar',
        'synopsis_short_en',
        'synopsis_short_ar',
        'duration',
    ],
    'media.xml': ['media_id', 'media_filename', 'image_width', 'image_height'],
    'genre.xml': ['genre_id', 'genre_en', 'genre_ar'],
    'program_media.xml': ['program_id', 'media_id'],
    'media_type.xml': ['media_type_id', 'media_generic', 'media_specific'],
    'region.xml': [
        'region_id',
        'region_type',
        'description',
        'short_description',
        'state',
        'sort_order',
    ],
    'region_service.xml': ['region_id', 'service_id', 'channel_number', 'channel_type'],
    'service_media.xml': [
        'service_id',
        'media_id',
        'media_location',
        'image_width',
        'image_height',
    ],
    'sub_genre.xml': ['sub_genre_id', 'sub_genre_en', 'sub_genre_ar'],
}

s3_resource = boto3.resource(
    service_name='s3',
    aws_access_key_id=ev_epg_s3_access_key_id,
    aws_secret_access_key=ev_epg_s3_secret_access_key,
)

s3_file_obj = s3_resource.Object(
    bucket_name=jv_src_bucket,
    key=jv_src_prefix.format(jv_env_name=jv_env_name, jv_env_type=jv_env_type) + jv_folder + '.zip',
)

buffer = io.BytesIO(s3_file_obj.get()["Body"].read())
z = zipfile.ZipFile(buffer)

for file in z.infolist():
    if file.filename in files_to_process:
        file_def = file_definitions[file.filename]
        file_obj = z.open(file)
        f_out = open(
            's3://'
            + ev_stg_s3_folder
            + '/'
            + jv_s3_sub_dir
            + '/'
            + jv_env_name
            + '/'
            + jv_folder
            + '/'
            + str.replace(file.filename, 'xml', 'csv'),
            'w',
            encoding='utf-8',
        )
        block = ''
        # header_written = False
        for line in file_obj.readlines():
            line = bytes.decode(line)
            line = line.strip()
            if block == '':
                if '<row>' in line:
                    block += line
            else:
                if '</row>' in line:
                    block += line
                    # process block
                    def_dict = OrderedDict()
                    for attribute in file_def:
                        def_dict[attribute] = ''
                    xml_tree = ElementTree.fromstring(block)
                    for col in xml_tree:
                        if col.text:
                            if ',' in col.text or '"' in col.text:
                                col_text = '"' + str.replace(col.text, '"', '""') + '"'
                            else:
                                col_text = col.text
                        else:
                            col_text = ''
                        if col.attrib:
                            def_dict[col.tag + '_' + col.attrib['lang']] = col_text
                        else:
                            def_dict[col.tag] = col_text
                        row = def_dict.values()
                    f_out.writelines(','.join(row) + '\n')
                    # reset block
                    block = ''
                else:
                    block += line
        f_out.close()
