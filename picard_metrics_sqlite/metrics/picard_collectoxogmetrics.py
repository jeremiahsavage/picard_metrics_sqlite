import os

from .metrics_util import picard_select_tsv_to_df

def picard_CollectOxoGMetrics_to_df(metric_path, logger):
    select = 'SAMPLE_ALIAS'
    df = picard_select_tsv_to_df(metric_path, select, logger)
    return df

def run(uuid, metric_path, bam, fasta, vcf, input_state, engine, logger, metric_name):
    table_name = 'picard_' + metric_name
    df = picard_CollectOxoGMetrics_to_df(metric_path, logger)
    df['bam'] = bam
    df['fasta'] = fasta
    df['input_state'] = input_state
    df['uuid'] = uuid
    df['vcf'] = vcf
    df.to_sql(table_name, engine, if_exists='append')
    return
