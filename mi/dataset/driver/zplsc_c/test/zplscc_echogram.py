from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.zplsc_c import ZplscCParser


def rec_exception_callback():
    return None


input_file_path = '/Users/admin/ZPLSC-temp/18030100.01A'
zplsc_echogram_file_path = '/Users/admin/ZPLSC-temp'

parser_config = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.zplsc_c',
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'ZplscCRecoveredDataParticle'}

with open(input_file_path, 'rb') as stream_handle:
    parser = ZplscCParser(parser_config, stream_handle, rec_exception_callback)
    parser.create_echogram(zplsc_echogram_file_path)
