import os

import .metrics_util

def picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger):
    select = 'CATEGORY'
    df = metrics_util.picard_select_tsv_to_df(stats_path, select, logger)
    return df


def run(uuid, stats_path, bam, fasta, input_state, engine, logger, metric_name):
    table_name = 'picard_' + metric_name
    df = picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger)
    if df is None:
        sys.exit('No metrics collected from: %s' % stats_path)
    else:
        df['uuid'] = uuid
        df['bam'] = bam
        df['input_state'] = input_state
        df['fasta'] = fasta
        table_name = table_name_list[i]
        df.to_sql(table_name, engine, if_exists='append')
    return
