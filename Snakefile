if 'outdir' not in config:
    config['outdir'] = 'output'

import re
import yaml
import os
import pandas as pd

def query2meta(query):
    query = re.sub('https://www.encodeproject.org/matrix', 'https://www.encodeproject.org/metadata', query)
    query = re.sub('&format=json', '', query)
    return query
def load_query_dict(yaml_file):
    with open(yaml_file, 'r') as f:
        dict_ = yaml.safe_load(f)
    for k in dict_.keys():
        dict_[k] = query2meta(dict_[k])
    return dict_

query_dict = load_query_dict(config['query_yaml'])

rule all:
    input:
        [ '{outdir}/merged_bed.{name_tag}.bed.gz'.format(name_tag=i, **config) for i in query_dict.keys() ],
        [ '{outdir}/merged_bed_exclude_unclassified.{name_tag}.bed.gz'.format(name_tag=i, **config) for i in query_dict.keys() ]

rule get_meta_data:
    output:
        '{outdir}/meta_data.{name_tag}.tsv'
    params:
        query = lambda wildcards: query_dict[wildcards.name_tag]
    shell:
        'wget -O {output[0]} "{params.query}"'
    
rule download_bed:
    input:
        '{outdir}/meta_data.{name_tag}.tsv'
    output:
        '{outdir}/bed_files.{name_tag}/'
    run:
        df = pd.read_csv(input[0], sep='\t')
        url_list = list(df['S3 URL'])
        for url in url_list:
            cmd = f'wget -nd -np -P {output[0]} {url}'
            os.system(cmd)

rule merge_bed:
    input:
        '{outdir}/bed_files.{name_tag}/'
    output:
        '{outdir}/merged_bed.{name_tag}.bed.gz',
    shell:
        'zcat `ls {input[0]}/*` | sort -k1,1 -k2,2n | {config[merge_bed_exe]} -i stdin -c 10 -o distinct| gzip > {output[0]}'

rule merge_bed_remove_unclassified:
    input:
        '{outdir}/bed_files.{name_tag}/'
    output:
        '{outdir}/merged_bed_exclude_unclassified.{name_tag}.bed.gz'
    shell:
        'zcat `ls {input[0]}/*` | grep -v Unclassified | sort -k1,1 -k2,2n | {config[merge_bed_exe]} -i stdin -c 10 -o distinct| gzip > {output[0]}'

