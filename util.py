
from ast import literal_eval

def get_cfg(cfg_file):
    with open(cfg_file, 'rb') as cfg_f:
        text = ''.join([line for line in cfg_f])
        params = literal_eval(text)
    return params

def add_table_suffixes(cfg, cfg_tmp):
    suffix = cfg['DATE_C']
    tmp = {key: value + '_' + suffix for key, value in cfg_tmp.items()}
    tmp['MODEL_PARAMS_FILE']=cfg_tmp['MODEL_PARAMS_FILE']
    conf = cfg
    return conf, tmp