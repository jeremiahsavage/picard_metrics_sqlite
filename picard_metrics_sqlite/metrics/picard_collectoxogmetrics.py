import os

import metrics_util

def picard_CollectOxoGMetrics_to_df(stats_path, logger):
    select = 'SAMPLE_ALIAS'
    df = metrics_util.picard_select_tsv_to_df(stats_path, select, logger)
    return df


def run(uuid, stats_path, bam, fasta, vcf, input_state, engine, logger, metric_name):
    stats_dir = os.path.dirname(stats_path)
    stats_name = os.path.basename(stats_path)
    stats_base, stats_ext = os.path.splitext(stats_name)

    df_list = list()
    table_name_list = list()

    table_name = 'picard_' + metric_name
    df = picard_CollectOxoGMetrics_to_df(stats_path, logger)
    if df is not None:
        df_list.append(df)
        table_name_list.append(table_name)
    for i, df in enumerate(df_list):
        df['uuid'] = uuid
        df['bam'] = bam
        df['input_state'] = input_state
        df['fasta'] = fasta
        df['vcf'] = vcf
        table_name = table_name_list[i]
        df.to_sql(table_name, engine, if_exists='append')
    return
