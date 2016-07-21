import sys

import pandas as pd


def picard_markduplicates_to_dict(uuid, bam, metrics_path, logger):
    data_dict = dict()
    read_header = False
    with open(metrics_path, 'r') as metrics_open:
        for line in metrics_open:
            if line.startswith("## HISTOGRAM"):
                break
            if line.startswith('#') or len(line) < 5:
                continue
            if not read_header:
                value_key_list = line.strip('\n').split('\t')
                logger.info('picard_markduplicates_to_dict() header value_key_list=\n\t%s' % value_key_list)
                logger.info('len(value_key_list=%s' % len(value_key_list))
                read_header = True
            else:
                data_list = line.strip('\n').split('\t')
                logger.info('picard_markduplicates_do_dict() data_list=\n\t%s' % data_list)
                logger.info('len(data_list)=%s' % len(data_list))
                for value_pos, value_key in enumerate(value_key_list):
                    data_dict[value_key] = data_list[value_pos]
    logger.info('picard_markduplicates data_dict=%s' % data_dict)
    return data_dict


    ## save stats to db
    if pipe_util.already_step(step_dir, 'picard_markduplicates_db', logger):
        logger.info('already stored `picard markduplicates` of %s to db' % bam_name)
    else:
        data_dict = picard_markduplicates_to_dict(uuid, bam_name, metrics_file, logger)
        data_dict['uuid'] = [uuid]
        data_dict['bam_name'] = bam_name
        data_dict['input_state'] = input_state
        df = pd.DataFrame(data_dict)
        table_name = 'picard_markduplicates'
        unique_key_dict = {'uuid': uuid, 'bam_name': bam_name}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(step_dir, 'picard_markduplicates_db', logger)
        logger.info('completed storing `picard markduplicates` of %s to db' % bam_name)
    return
