import os

from .metrics_util import picard_select_tsv_to_df

def picard_CollectHsMetrics_to_df(stats_path, logger):
    select = 'BAIT_SET'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df

def picard_CollectHsMetrics_histogram_to_df(stats_path, logger):
    select = 'coverage'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df

def run(uuid, stats_path, bam, bam_library, bait, target, fasta, input_state, engine, logger, metric_name):
    table_name = 'picard_' + metric_name
    df = picard_CollectHsMetrics_to_df(stats_path, logger)
    df['uuid'] = uuid
    df['bam'] = bam
    df['library'] = bam_library
    df['bait'] = bait
    df['target'] = target
    df['input_state'] = input_state
    df['fasta'] = fasta
    df.to_sql(table_name, engine, if_exists='append')

    table_name = 'picard_' + metric_name + '_histogram'
    df = picard_CollectHsMetrics_histogram_to_df(stats_path, logger)
    df['uuid'] = uuid
    df['bam'] = bam
    df['library'] = bam_library
    df['bait'] = bait
    df['target'] = target
    df['input_state'] = input_state
    df['fasta'] = fasta
    df.to_sql(table_name, engine, if_exists='append')    
    return
