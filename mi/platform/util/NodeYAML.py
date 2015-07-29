import copy

from mi.platform.exceptions import NodeConfigurationFileException

class NodeYAML(object):
    def factory(node_config):
        if node_config is None:
            return NullNodeYAML(node_config={'node_meta_data': {},
                                             'node_streams': {},
                                             'node_port_info': {},})
        if "port_info" in node_config.keys():
            return PortNodeYAML(node_config=node_config)
        else:
            return NoPortNodeYAML(node_config=node_config)

    factory = staticmethod(factory)


    def __init__(self, *args, **kwargs):
        node_config = kwargs.pop('node_config')
        super(NodeYAML, self).__init__(*args, **kwargs)

        self._node_meta_data = copy.deepcopy(node_config["node_meta_data"])
        self._node_streams = copy.deepcopy(node_config["node_streams"])


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
        scale_factor = 'scale_factor'
        monitor_cycle_seconds = 'monitor_cycle_seconds'
        ion_parameter_name = 'ion_parameter_name'
        stream_parameters = [ion_parameter_name,
                             monitor_cycle_seconds,
                             scale_factor,
                             ]

        for stream in self.node_streams:
            for attr in self.node_streams[stream]:
                for stream_parameter in stream_parameters:
                    if not self.node_streams[stream][attr].get(stream_parameter, None):
                        raise NodeConfigurationFileException(msg="Stream: '%s', Attribute: '%s', is missing '%s'" % (stream, attr, stream_parameter))

                if not isinstance(self.node_streams[stream][attr][ion_parameter_name], str):
                    raise NodeConfigurationFileException(msg="Stream: '%s', Attribute: '%s', %s is not a string" % (stream, attr, ion_parameter_name))

                if not isinstance(self.node_streams[stream][attr][monitor_cycle_seconds], (int, long, float)):
                    raise NodeConfigurationFileException(msg="Stream: '%s', Attribute: '%s', %s is not a numeric type" % (stream, attr, monitor_cycle_seconds))
                    
                if not isinstance(self.node_streams[stream][attr][scale_factor], (int, long, float)):
                    raise NodeConfigurationFileException(msg="Stream: '%s', Attribute: '%s', %s is not a numeric type" % (stream, attr, scale_factor))

    def _validate_node_port_info(self):
        port_oms_port_cntl_id = 'port_oms_port_cntl_id'
        for port_id, port_dict in self.node_port_info.iteritems():
            if not port_dict or not port_oms_port_cntl_id in port_dict:
                raise NodeConfigurationFileException(msg="%s is missing from %s" % (port_oms_port_cntl_id, port_id))

            if not isinstance(port_dict[port_oms_port_cntl_id], int):
                raise NodeConfigurationFileException(msg="%s value is not an int for %s" % (port_oms_port_cntl_id, port_id))


class NullNodeYAML(NodeYAML):
    def validate(self):
        raise NodeConfigurationFileException(msg="This a null configuration.")

class NoPortNodeYAML(NodeYAML):
    def __init__(self, *args, **kwargs):
        self._node_port_info = {'J99': {'port_oms_port_cntl_id':99}}
        super(NoPortNodeYAML, self).__init__(*args, **kwargs)

    def validate(self):
        self._validate_node_meta_data()
        self._validate_node_streams()


class PortNodeYAML(NodeYAML):

    def __init__(self, *args, **kwargs):
        node_config = kwargs.get('node_config')
        self._node_port_info = copy.deepcopy(node_config["port_info"])
        super(PortNodeYAML, self).__init__(*args, **kwargs)


