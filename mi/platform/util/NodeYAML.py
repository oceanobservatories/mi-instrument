import copy
from collections import MutableMapping, namedtuple

from mi.platform.exceptions import NodeConfigurationFileException, StreamConfigurationFileException

ATTR = 'attribute'
OMC_NAME = 'omc_parameter_name'
ION_NAME = 'ion_parameter_name'
SCALE = 'scale_factor'
MON_SEC = 'monitor_cycle_seconds'
NULL_CONFIG = {'node_meta_data': {},
               'node_streams': {},
               'node_port_info': {}, }


RealizedParameter = namedtuple('RealizedParameter', [ATTR, ION_NAME, SCALE, MON_SEC])


class NodeYAML(object):
    @staticmethod
    def factory(node_config, stream_definitions):
        if node_config is None:
            return NullNodeYAML()
        if "port_info" in node_config.keys():
            return PortNodeYAML(node_config, stream_definitions)
        else:
            return NoPortNodeYAML(node_config, stream_definitions)

    def __init__(self, node_config, stream_definitions):
        if stream_definitions:
            self._stream_definitions = Streams(stream_definitions)
            self._stream_definitions.validate()
            self._node_streams = self._create_streams(node_config["node_streams"])
        else:
            self._stream_definitions = None
            self._node_streams = None

        self._node_meta_data = node_config["node_meta_data"]
        self._node_port_info = None

    @property
    def node_meta_data(self):
        return self._node_meta_data

    @property
    def node_streams(self):
        return self._node_streams

    @property
    def node_port_info(self):
        return self._node_port_info

    def validate(self):
        self._validate_node_meta_data()
        self._validate_node_streams()
        self._validate_node_port_info()

    def _create_streams(self, node_streams):
        results = {}
        for stream in node_streams:
            for var_map in node_streams[stream]:
                name = var_map['name']
                results.setdefault(stream, {})[name] = self._stream_definitions[stream].get_parameters(var_map)
        return results

    def _validate_node_meta_data(self):
        nms_source = 'nms_source'
        oms_sample_rate = 'oms_sample_rate'
        meta_data = ['node_id_name',
                     'description',
                     'location',
                     'reference_designator',
                     ]

        for meta_data_item in meta_data:
            if not self.node_meta_data.get(meta_data_item, None):
                raise NodeConfigurationFileException(msg="%s is missing from config file" % meta_data_item)

        if not self.node_meta_data.get(nms_source, None):
            self.node_meta_data[nms_source] = 0

        if not self.node_meta_data.get(oms_sample_rate, None):
            self.node_meta_data[oms_sample_rate] = 60

    def _validate_node_streams(self):
        pass

    def _validate_node_port_info(self):
        port_oms_port_cntl_id = 'port_oms_port_cntl_id'
        for port_id, port_dict in self.node_port_info.iteritems():
            if not port_dict or not port_oms_port_cntl_id in port_dict:
                raise NodeConfigurationFileException(msg="%s is missing from %s" % (port_oms_port_cntl_id, port_id))

            if not isinstance(port_dict[port_oms_port_cntl_id], int):
                raise NodeConfigurationFileException(
                    msg="%s value is not an int for %s" % (port_oms_port_cntl_id, port_id))


class NullNodeYAML(object):
    def validate(self):
        raise NodeConfigurationFileException(msg="This a null configuration.")


class NoPortNodeYAML(NodeYAML):
    def __init__(self, node_config, stream_definitions):
        super(NoPortNodeYAML, self).__init__(node_config, stream_definitions)
        self._node_port_info = {'J99': {'port_oms_port_cntl_id': 99}}

    def validate(self):
        self._validate_node_meta_data()
        self._validate_node_streams()


class PortNodeYAML(NodeYAML):
    def __init__(self, node_config, stream_definitions):
        super(PortNodeYAML, self).__init__(node_config, stream_definitions)
        self._node_port_info = node_config["port_info"]


class ParameterException(Exception):
    pass


class ParameterDefinition(object):
    def __init__(self, parameter_dict):
        self.omc_parameter_name = parameter_dict.get(OMC_NAME)
        self.ion_parameter_name = parameter_dict.get(ION_NAME)
        self.monitor_cycle_seconds = parameter_dict.get(MON_SEC)
        self.scale_factor = parameter_dict.get(SCALE, 1)

    def validate(self):
        if not isinstance(self.omc_parameter_name, basestring):
            raise ParameterException(msg="omc_parameter_name, %s is not a string" % self.omc_parameter_name)

        if not isinstance(self.ion_parameter_name, basestring):
            raise ParameterException(msg="ion_parameter_name, %s is not a string" % self.ion_parameter_name)

        if not isinstance(self.monitor_cycle_seconds, (int, long, float)):
            raise ParameterException(msg="monitor_cycle_seconds, %s is not a numeric type" % self.monitor_cycle_seconds)

        if not isinstance(self.scale_factor, (int, long, float)):
            raise ParameterException(msg="scale_factor, %s is not a numeric type" % self.scale_factor)

    def format(self, var_map):
        name = self.omc_parameter_name
        for k, v in var_map.iteritems():
            name = name.replace('{%s}' % k, str(v))

        return name, RealizedParameter(name, self.ion_parameter_name, self.scale_factor, self.monitor_cycle_seconds)


class StreamConfig(object):
    def __init__(self, stream_definition):
        self.variables = stream_definition.get('variables', None)
        if self.variables is None:
            self.variables = []
        self.parameters = [ParameterDefinition(p) for p in stream_definition.get('parameters')]

    def get_parameters(self, var_map):
        for variable in self.variables:
            if variable not in var_map:
                raise StreamConfigurationFileException('Missing mandatory variable from configuration')

        params = {}
        for parameter in self.parameters:
            name, value = parameter.format(var_map)
            params[name] = value
        return params

    def validate(self):
        [p.validate() for p in self.parameters]


class Streams(MutableMapping):
    def __init__(self, stream_config):
        self._streams = {}
        for stream in stream_config['streams']:
            self._streams[stream] = StreamConfig(stream_config['streams'][stream])

    def __setitem__(self, key, value):
        return self._streams.__setitem__(key, value)

    def __len__(self):
        return self._streams.__len__()

    def __iter__(self):
        return self._streams.__iter__()

    def __getitem__(self, key):
        return self._streams.__getitem__(key)

    def __delitem__(self, key):
        return self._streams.__delitem__(key)

    def validate(self):
        [s.validate() for s in self._streams.itervalues()]
