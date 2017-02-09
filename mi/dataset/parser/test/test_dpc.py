import glob
import os
from mock import Mock
from nose.plugins.attrib import attr
from mi.dataset.parser.dpc import DeepProfilerParticle, DeepProfilerParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.core.log import get_logger

log = get_logger()

__author__ = 'pcable'

TEST_DIR = os.path.dirname(__file__)
RESOURCE_DIR = os.path.abspath(os.path.join(TEST_DIR, '../../driver/dpc/resource'))

TEST_DATA = [
    [1388886616, 722537, {'doconcs': 37.921, 't': 7.818}],
    [1388886615, 464278, {'chlaflo': 85, 'ntuflo': 247}],
    [1388886615, 410319, {'cdomflo': 168}],
    [1388886625,
      103951,
      {'current': 0.0,
       'mode': 'down',
       'pnum': 6400,
       'pressure': 14.3,
       'vbatt': 12.1}],
    [1388886615,
      528651,
      {'hx': 0.3263,
       'hy': 0.1313,
       'hz': -0.9361,
       'tx': -1.52,
       'ty': 0.31,
       'va': 4.11,
       'vb': -3.28,
       'vc': -0.79,
       'vd': -0.89}],
    [1388886623,
      522393,
      {'condwat': 32.2808, 'preswat': 14.28, 'tempwat': 7.8077}],
    [1388886623,
      522393,
      '\x05' '\x01' '\x53' '\x00' '\x00' '\x8d' '\x01' '\xd3' '\x7f' '\x6f' '\x07' '\x14' '\x75' '\x02' '\xad' '\xbf'
      '\x01' '\xe1' '\x02' '\xbc' '\x00' '\x00' '\x2b' '\xb3' '\x01' '\x55' '\x04' '\x8d' '\x04' '\x5f' '\x01' '\xeb'
      '\x01' '\x0d' '\x05' '\x3a' '\x05' '\x07' '\x02' '\x48' '\x01' '\x9e' '\x05' '\xf1' '\x05' '\xb9' '\x02' '\xb4'
      '\x02' '\x42' '\x06' '\xb1' '\x06' '\x75' '\x03' '\x30' '\x02' '\xf0' '\x07' '\x81' '\x07' '\x3f' '\x03' '\xb8'
      '\x03' '\x9a' '\x08' '\x5d' '\x08' '\x0f' '\x04' '\x4f' '\x04' '\x39' '\x09' '\x4b' '\x08' '\xf6' '\x04' '\xfa'
      '\x04' '\xdc' '\x0a' '\x57' '\x09' '\xec' '\x05' '\xba' '\x05' '\x7e' '\x0b' '\x6d' '\x0a' '\xfa' '\x06' '\x91'
      '\x06' '\x27' '\x0c' '\x9d' '\x0c' '\x1d' '\x07' '\x78' '\x06' '\xdf' '\x0d' '\xe2' '\x0d' '\x52' '\x08' '\x7d'
      '\x07' '\xa5' '\x0f' '\x36' '\x0e' '\x9b' '\x09' '\x95' '\x08' '\x80' '\x10' '\x9b' '\x0f' '\xec' '\x0a' '\xbf'
      '\x09' '\x6b' '\x12' '\x1a' '\x11' '\x51' '\x0c' '\x04' '\x0a' '\x70' '\x13' '\xc9' '\x12' '\xdb' '\x0d' '\x73'
      '\x0b' '\xa2' '\x15' '\xb4' '\x14' '\x96' '\x0f' '\x18' '\x0d' '\x0b' '\x17' '\xe4' '\x16' '\x99' '\x11' '\x05'
      '\x0e' '\xbc' '\x1a' '\x3c' '\x18' '\xd0' '\x13' '\x14' '\x10' '\x9f' '\x1c' '\xb9' '\x1b' '\x26' '\x15' '\x51'
      '\x12' '\xa6' '\x1f' '\x5f' '\x1d' '\xab' '\x17' '\xb7' '\x14' '\xda' '\x22' '\x24' '\x20' '\x48' '\x1a' '\x46'
      '\x17' '\x2d' '\x25' '\x05' '\x23' '\x02' '\x1c' '\xff' '\x19' '\x9e' '\x27' '\xf0' '\x25' '\xd0' '\x1f' '\xce'
      '\x1c' '\x22' '\x2a' '\xe7' '\x28' '\xa5' '\x22' '\xae' '\x1e' '\xb0' '\x2d' '\xd0' '\x2b' '\x78' '\x25' '\x9b'
      '\x21' '\x44' '\x30' '\xb7' '\x2e' '\x40' '\x28' '\x8a' '\x23' '\xd4' '\x33' '\x98' '\x31' '\x00' '\x2b' '\x7f'
      '\x26' '\x65' '\x36' '\x7e' '\x33' '\xc7' '\x2e' '\x80' '\x29' '\x09' '\x39' '\x7d' '\x36' '\x92' '\x31' '\xa0'
      '\x2b' '\xc2' '\x3c' '\x97' '\x39' '\x7b' '\x34' '\xe7' '\x2e' '\x9d' '\x3f' '\xe1' '\x3c' '\x87' '\x38' '\x59'
      '\x31' '\xad' '\x43' '\x5e' '\x3f' '\xc4' '\x3c' '\x08' '\x35' '\x02' '\x47' '\x05' '\x43' '\x30' '\x3f' '\xef'
      '\x38' '\x9f' '\x4a' '\xe9' '\x46' '\xd2' '\x44' '\x14' '\x3c' '\x7a' '\x4e' '\xdf' '\x4a' '\x9b' '\x48' '\x66'
      '\x40' '\x89' '\x52' '\xcb' '\x4e' '\x6a' '\x4c' '\xad' '\x44' '\xab' '\x56' '\x94' '\x52' '\x19' '\x50' '\xe0'
      '\x48' '\xba' '\x5a' '\x25' '\x55' '\x94' '\x54' '\xe8' '\x4c' '\xa5' '\x5d' '\x84' '\x58' '\xe0' '\x58' '\xc0'
      '\x50' '\x6b' '\x60' '\xc6' '\x5c' '\x08' '\x5c' '\xa1' '\x54' '\x13' '\x63' '\xd8' '\x5f' '\x06' '\x60' '\x67'
      '\x57' '\x95' '\x68' '\x5a' '\x62' '\x09' '\x65' '\x0e' '\x5a' '\xfc' '\x6b' '\x6d' '\x65' '\x11' '\x68' '\xaf'
      '\x5e' '\xa3' '\x6e' '\x50' '\x67' '\xe7' '\x6c' '\x29' '\x62' '\x21' '\x71' '\x10' '\x6a' '\xa0' '\x6f' '\x8e'
      '\x65' '\x88' '\x73' '\xa1' '\x6d' '\x31' '\x72' '\xce' '\x68' '\xcd' '\x75' '\xfa' '\x6f' '\x87' '\x75' '\xd1'
      '\x6b' '\xd8' '\x78' '\x23' '\x71' '\xb3' '\x78' '\xb9' '\x6e' '\xc5' '\x79' '\xfd' '\x73' '\xa3' '\x7b' '\x51'
      '\x71' '\x81' '\x7b' '\x7c' '\x75' '\x3d' '\x7d' '\x91' '\x73' '\xf4' '\x7c' '\xba' '\x76' '\x8b' '\x7f' '\x8f'
      '\x76' '\x28' '\x7d' '\x9e' '\x77' '\x96' '\x81' '\x3d' '\x78' '\x26' '\x7e' '\x15' '\x78' '\x41' '\x82' '\x6e'
      '\x79' '\xcf' '\x7e' '\x37' '\x78' '\x8b' '\x83' '\x42' '\x7b' '\x28' '\x7e' '\x17' '\x78' '\x8b' '\x83' '\xe3'
      '\x7c' '\x3a' '\x7d' '\x86' '\x78' '\x3c' '\x84' '\x06' '\x7c' '\xfb' '\x7c' '\xa1' '\x77' '\x86' '\x83' '\xbc'
      '\x7d' '\x51' '\x7b' '\x6f' '\x76' '\x84' '\x83' '\x27' '\x7d' '\x53' '\x79' '\xeb' '\x75' '\x34' '\x82' '\x2d'
      '\x7c' '\xfd' '\x78' '\x1f' '\x73' '\x9b' '\x80' '\xdb' '\x7c' '\x44' '\x76' '\x1d' '\x71' '\xc6' '\x7f' '\x53'
      '\x7b' '\x3a' '\x73' '\xcc' '\x6f' '\xbb' '\x7d' '\x66' '\x79' '\xdf' '\x71' '\x3e' '\x6d' '\x5e' '\x7b' '\x26'
      '\x78' '\x18' '\x6e' '\x6f' '\x6a' '\xcd' '\x78' '\x9c' '\x76' '\x04' '\x6b' '\x52' '\x67' '\xfd' '\x75' '\xae'
      '\x73' '\x95' '\x67' '\xe4' '\x64' '\xdf' '\x72' '\x56' '\x70' '\xba' '\x64' '\x2e' '\x61' '\x7c' '\x6e' '\x9d'
      '\x6d' '\x81' '\x60' '\x46' '\x5d' '\xce' '\x6a' '\x92' '\x69' '\xe5' '\x5c' '\x35' '\x59' '\xfe' '\x66' '\x59'
      '\x66' '\x18' '\x58' '\x11' '\x56' '\x0e' '\x61' '\xf4' '\x62' '\x1e' '\x53' '\xec' '\x52' '\x0b' '\x5d' '\x80'
      '\x5e' '\x01' '\x4f' '\xcd' '\x4e' '\x0f' '\x59' '\x0a' '\x59' '\xdd' '\x4b' '\xbb' '\x4a' '\x1d' '\x54' '\x95'
      '\x55' '\xb8' '\x47' '\xb2' '\x46' '\x36' '\x50' '\x2c' '\x51' '\x8e' '\x43' '\xbe' '\x42' '\x5f' '\x4b' '\xcd'
      '\x4d' '\x6c' '\x3f' '\xe9' '\x3e' '\xa3' '\x47' '\x93' '\x49' '\x5e' '\x3c' '\x2d' '\x3a' '\xfe' '\x43' '\x79'
      '\x45' '\x5d' '\x38' '\x90' '\x37' '\x79' '\x3f' '\x7a' '\x41' '\x70' '\x35' '\x22' '\x34' '\x14' '\x3b' '\xab'
      '\x3d' '\x96' '\x31' '\xca' '\x30' '\xd9' '\x37' '\xff' '\x39' '\xda' '\x2e' '\x96' '\x2d' '\xbc' '\x34' '\x6e'
      '\x36' '\x37' '\x2b' '\x8a' '\x2a' '\xb7' '\x31' '\x11' '\x32' '\xaa' '\x28' '\x9f' '\x27' '\xe2' '\x2d' '\xe1'
      '\x2f' '\x54' '\x25' '\xd4' '\x25' '\x2c' '\x2a' '\xcc' '\x2c' '\x23' '\x22' '\xe4' '\x22' '\x91' '\x27' '\xca'
      '\x29' '\x14']
    ]

# these values were verified in https://docs.google.com/spreadsheets/d/1w1x60fCuhgSCOJxBh_4NVL08Uz-XF1X6pUVMdvoyZr8/edit?usp=sharing

TEST_RESULTS = {
    'dpc_optode_instrument_recovered': {'internal_timestamp': 3597875416.722537,
                              'pkt_format_id': 'JSON_Data',
                              'pkt_version': 1,
                              'preferred_timestamp': 'internal_timestamp',
                              'quality_flag': 'ok',
                              'stream_name': 'dpc_optode_instrument',
                              'values': [{'value': 1388886616, 'value_id': 'raw_time_seconds'},
                                         {'value': 722537, 'value_id': 'raw_time_microseconds'},
                                         {'value': 37.921, 'value_id': 'calibrated_phase'},
                                         {'value': 7.818, 'value_id': 'optode_temperature'}]},
    'dpc_flnturtd_instrument_recovered': {'internal_timestamp': 3597875415.464278,
                                'pkt_format_id': 'JSON_Data',
                                'pkt_version': 1,
                                'preferred_timestamp': 'internal_timestamp',
                                'quality_flag': 'ok',
                                'stream_name': 'dpc_flnturtd_instrument',
                                'values': [{'value': 1388886615, 'value_id': 'raw_time_seconds'},
                                           {'value': 464278, 'value_id': 'raw_time_microseconds'},
                                           {'value': 247, 'value_id': 'ntuflo'},
                                           {'value': 85, 'value_id': 'chlaflo'}]},
    'dpc_flcdrtd_instrument_recovered': {'internal_timestamp': 3597875415.410319,
                               'pkt_format_id': 'JSON_Data',
                               'pkt_version': 1,
                               'preferred_timestamp': 'internal_timestamp',
                               'quality_flag': 'ok',
                               'stream_name': 'dpc_flcdrtd_instrument',
                               'values': [{'value': 1388886615, 'value_id': 'raw_time_seconds'},
                                          {'value': 410319, 'value_id': 'raw_time_microseconds'},
                                          {'value': 168, 'value_id': 'cdomflo'}]},
    'dpc_mmp_instrument_recovered': {'internal_timestamp': 3597875425.103951,
                'pkt_format_id': 'JSON_Data',
                'pkt_version': 1,
                'preferred_timestamp': 'internal_timestamp',
                'quality_flag': 'ok',
                'stream_name': 'dpc_mmp_instrument',
                'values': [{'value': 1388886625, 'value_id': 'raw_time_seconds'},
                           {'value': 103951, 'value_id': 'raw_time_microseconds'},
                           {'value': 0.0, 'value_id': 'wfp_prof_current'},
                           {'value': 14.3, 'value_id': 'wfp_prof_pressure'},
                           {'value': 12.1, 'value_id': 'wfp_prof_voltage'},
                           {'value': 6400, 'value_id': 'wfp_profile_number'},
                           {'value': 'down', 'value_id': 'operating_mode'}]},
    'dpc_acm_instrument_recovered': {'internal_timestamp': 3597875415.528651,
                           'pkt_format_id': 'JSON_Data',
                           'pkt_version': 1,
                           'preferred_timestamp': 'internal_timestamp',
                           'quality_flag': 'ok',
                           'stream_name': 'dpc_acm_instrument',
                           'values': [{'value': 1388886615, 'value_id': 'raw_time_seconds'},
                                      {'value': 528651, 'value_id': 'raw_time_microseconds'},
                                      {'value': -0.9361, 'value_id': 'vel3d_a_hz'},
                                      {'value': 4.11, 'value_id': 'vel3d_a_va'},
                                      {'value': 0.3263, 'value_id': 'vel3d_a_hx'},
                                      {'value': 0.1313, 'value_id': 'vel3d_a_hy'},
                                      {'value': -0.89, 'value_id': 'vel3d_a_vd'},
                                      {'value': -1.52, 'value_id': 'vel3d_a_tx'},
                                      {'value': 0.31, 'value_id': 'vel3d_a_ty'},
                                      {'value': -0.79, 'value_id': 'vel3d_a_vc'},
                                      {'value': -3.28, 'value_id': 'vel3d_a_vb'}]},
    'dpc_ctd_instrument_recovered': {'internal_timestamp': 3597875423.522393,
                           'pkt_format_id': 'JSON_Data',
                           'pkt_version': 1,
                           'preferred_timestamp': 'internal_timestamp',
                           'quality_flag': 'ok',
                           'stream_name': 'dpc_ctd_instrument',
                           'values': [{'value': 1388886623, 'value_id': 'raw_time_seconds'},
                                      {'value': 522393, 'value_id': 'raw_time_microseconds'},
                                      {'value': 7.8077, 'value_id': 'temp'},
                                      {'value': 14.28, 'value_id': 'pressure'},
                                      {'value': 32.2808, 'value_id': 'conductivity_millisiemens'}]},
    'dpc_acs_instrument_recovered': {'internal_timestamp': 3597875423.522393,
                           'pkt_format_id': 'JSON_Data',
                           'pkt_version': 1,
                           'preferred_timestamp': 'internal_timestamp',
                           'quality_flag': 'ok',
                           'stream_name': 'dpc_acs_instrument',
                           'values': [{'value': 1388886623, 'value_id': 'raw_time_seconds'},
                                      {'value': 522393, 'value_id': 'raw_time_microseconds'},
                                      {'value': 5, 'value_id': 'packet_type'},
                                      {'value': 83, 'value_id': 'meter_type'},
                                      {'value': '141', 'value_id': 'serial_number'},
                                      {'value': 467, 'value_id': 'a_reference_dark_counts'},
                                      {'value': 32623, 'value_id': 'pressure_counts'},
                                      {'value': 1812, 'value_id': 'a_signal_dark_counts'},
                                      {'value': 29954, 'value_id': 'external_temp_raw'},
                                      {'value': 44479, 'value_id': 'internal_temp_raw'},
                                      {'value': 481, 'value_id': 'c_reference_dark_counts'},
                                      {'value': 700, 'value_id': 'c_signal_dark_counts'},
                                      {'value': 11187, 'value_id': 'elapsed_run_time'},
                                      {'value': 85, 'value_id': 'num_wavelengths'},
                                      {'value': [491, 584, 692, 816, 952, 1103, 1274, 1466, 1681, 1912, 2173, 2453,
                                                 2751, 3076, 3443, 3864, 4357, 4884, 5457, 6071, 6726, 7423, 8142, 8878,
                                                 9627, 10378, 11135, 11904, 12704, 13543, 14425, 15368, 16367, 17428,
                                                 18534, 19629, 20704, 21736, 22720, 23713, 24679, 25870, 26799, 27689,
                                                 28558, 29390, 30161, 30905, 31569, 32145, 32655, 33085, 33390, 33602,
                                                 33763, 33798, 33724, 33575, 33325, 32987, 32595, 32102, 31526, 30876,
                                                 30126, 29270, 28317, 27282, 26201, 25076, 23936, 22794, 21653, 20524,
                                                 19405, 18323, 17273, 16250, 15275, 14335, 13422, 12561, 11745, 10956,
                                                 10186],'value_id': 'c_signal_counts'},
                                      {'value': [269, 414, 578, 752, 922, 1081, 1244, 1406, 1575, 1759, 1957, 2176,
                                                 2411, 2672, 2978, 3339, 3772, 4255, 4774, 5338, 5933, 6558, 7202, 7856,
                                                 8516, 9172, 9829, 10505, 11202, 11933, 12717, 13570, 14495, 15482,
                                                 16521, 17579, 18618, 19621, 20587, 21523, 22421, 23292, 24227, 25121,
                                                 25992, 26829, 27608, 28357, 29057, 29684, 30248, 30758, 31183, 31528,
                                                 31802, 31995, 32081, 32083, 31997, 31812, 31546, 31199, 30744, 30212,
                                                 29589, 28858, 28033, 27109, 26136, 25118, 24065, 23005, 21944, 20878,
                                                 19820, 18782, 17757, 16752, 15766, 14810, 13879, 12970, 12116, 11299,
                                                 10516], 'value_id': 'a_signal_counts'},
                                      {'value': [1165, 1338, 1521, 1713, 1921, 2141, 2379, 2647, 2925, 3229, 3554, 3894,
                                                 4251, 4634, 5065, 5556, 6116, 6716, 7353, 8031, 8740, 9477, 10224,
                                                 10983, 11728, 12471, 13208, 13950, 14717, 15511, 16353, 17246, 18181,
                                                 19177, 20191, 21195, 22164, 23077, 23940, 24774, 25560, 26714, 27501,
                                                 28240, 28944, 29601, 30202, 30755, 31229, 31612, 31930, 32158, 32277,
                                                 32311, 32279, 32134, 31905, 31599, 31211, 30751, 30237, 29644, 28990,
                                                 28271, 27474, 26596, 25646, 24646, 23605, 22545, 21484, 20429, 19387,
                                                 18354, 17342, 16361, 15405, 14480, 13602, 12746, 11926, 11146, 10399,
                                                 9684, 8932], 'value_id': 'c_reference_counts'},
                                      {'value': [1119, 1287, 1465, 1653, 1855, 2063, 2294, 2540, 2810, 3101, 3410, 3739,
                                                 4076, 4433, 4827, 5270, 5785, 6352, 6950, 7595, 8264, 8962, 9680,
                                                 10405, 11128, 11840, 12544, 13255, 13970, 14715, 15495, 16324, 17200,
                                                 18130, 19099, 20074, 21017, 21908, 22752, 23560, 24326, 25097, 25873,
                                                 26599, 27296, 27953, 28551, 29107, 29603, 30013, 30347, 30614, 30785,
                                                 30859, 30859, 30780, 30598, 30340, 30004, 29595, 29126, 28603, 27998,
                                                 27341, 26621, 25823, 24956, 24014, 23038, 22030, 21003, 19983, 18973,
                                                 17974, 16991, 16035, 15102, 14201, 13332, 12505, 11708, 10935, 10210,
                                                 9516, 8849], 'value_id': 'a_reference_counts'}]},
}


@attr('UNIT', group='mi')
class DeepProfilerUnitTestCase(ParserUnitTestCase):
    def flatten(self, particle):
        d = {}
        for each in particle.get('values', []):
            d[each.get('value_id')] = each.get('value')
        return d

    def test_particles(self):
        self.maxDiff = None
        for data in TEST_DATA:
            particle = DeepProfilerParticle(data, preferred_timestamp="internal_timestamp").generate_dict()
            particle_type = particle.get('stream_name')
            self.assertDictEqual(self.flatten(particle), self.flatten(TEST_RESULTS[particle_type]))
            self.assertEqual(particle.get('preferred_timestamp'), TEST_RESULTS[particle_type].get('preferred_timestamp'))

    def test_recovered_particles(self):
        for f in glob.glob(os.path.join(RESOURCE_DIR, '*.mpk')):
            log.debug('testing file: %s', f)
            with open(f, 'rb') as fh:
                parser = DeepProfilerParser({}, fh, Mock())
                particles = []
                while True:
                    p = parser.get_records(100)
                    if p:
                        particles.extend(p)
                    else:
                        break
                yml_file = f.replace('.mpk', '.yml')
                self.assert_particles(particles[:1], yml_file)

