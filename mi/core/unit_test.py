#! /usr/bin/env python

"""
@file ion/core/unit_test.py
@author Bill French
@brief Base test class for all MI tests.  Provides two base classes, 
One for pyon tests and one for stand alone MI tests. 

We have the stand alone test case for tests that don't require or can't
integrate with the common ION test case.
"""


from mi.core.log import get_logger
log = get_logger()

import unittest
import json 

from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import DataParticleValue
from mi.idk.exceptions import IDKException



import os

class PyonTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self.addCleanup(self._file_sys_clean)
    # Call this function at the beginning of setUp if you need a mock ion
    # obj

    # @see
    # http://www.saltycrane.com/blog/2012/07/how-prevent-nose-unittest-using-docstring-when-verbosity-2/
    def shortDescription(self):
        return None

    # override __str__ and __repr__ behavior to show a copy-pastable nosetest name for ion tests
    #  ion.module:TestClassName.test_function_name
    def __repr__(self):
        name = self.id()
        name = name.split('.')
        if name[0] not in ["ion", "pyon"]:
            return "%s (%s)" % (name[-1], '.'.join(name[:-1]))
        else:
            return "%s ( %s )" % (name[-1], '.'.join(name[:-2]) + ":" + '.'.join(name[-2:]))
    __str__ = __repr__

    def _create_IonObject_mock(self, name):
        mock_ionobj = Mock(name='IonObject')
        def side_effect(_def, _dict=None, **kwargs):
            test_obj = IonObject(_def, _dict, **kwargs)
            test_obj._validate()
            return DEFAULT
        mock_ionobj.side_effect = side_effect
        patcher = patch(name, mock_ionobj)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def _file_sys_clean(self):
        pass
        # if os.environ.get('CEI_LAUNCH_TEST', None) is None:
        #     FileSystem._clean(CFG)



    def _create_service_mock(self, service_name):
        # set self.clients if not already set
        clients = Mock(name='clients')
        base_service = get_service_registry().get_service_base(service_name)
        # Save it to use in test_verify_service
        self.base_service = base_service
        self.addCleanup(delattr, self, 'base_service')
        dependencies = base_service.dependencies
        for dep_name in dependencies:
            dep_service = get_service_registry().get_service_base(dep_name)
            # Force mock service to use interface
            mock_service = Mock(name='clients.%s' % dep_name,
                    spec=dep_service)
            setattr(clients, dep_name, mock_service)
            # set self.dep_name for conevenience
            setattr(self, dep_name, mock_service)
            self.addCleanup(delattr, self, dep_name)
            iface = list(implementedBy(dep_service))[0]
            names_and_methods = iface.namesAndDescriptions()
            for func_name, _ in names_and_methods:
                mock_func = mocksignature(getattr(dep_service, func_name),
                        mock=Mock(name='clients.%s.%s' % (dep_name,
                            func_name)), skipfirst=True)
                setattr(mock_service, func_name, mock_func)
        return clients

    # Assuming your service is the only subclass of the Base Service
    def test_verify_service(self):
        if not getattr(self, 'base_service', None):
            raise unittest.SkipTest('Not implementing an Ion Service')
        from zope.interface.verify import verifyClass
        base_service = self.base_service
        implemented_service = base_service.__subclasses__()[0]
        iface = list(implementedBy(base_service))[0]
        verifyClass(iface, implemented_service)
        # Check if defined functions in Base Service are all implemented
        difference = set(func_names(base_service)) - set(func_names(implemented_service)) - set(['__init__'])
        if difference:
            self.fail('Following function declarations in %s do not exist in %s : %s' %
                    (iface, implemented_service,
                        list(difference)))

    def patch_cfg(self, cfg_obj_or_str, *args, **kwargs):
        """
        Helper method for patching the CFG (or any dict, but useful for patching CFG).

        This method exists because the decorator versions of patch/patch.dict do not function
        until the test_ method is called - ie, when setUp is run, the patch hasn't occured yet.
        Use this in your setUp method if you need to patch CFG and have stuff in setUp respect it.

        @param  cfg_obj_or_str  An actual ref to CFG or a string defining where to find it ie 'pyon.ion.exchange.CFG'
        @param  *args           *args to pass to patch.dict
        @param  **kwargs        **kwargs to pass to patch.dict
        """
        patcher = patch.dict(cfg_obj_or_str, *args, **kwargs)
        patcher.start()
        self.addCleanup(patcher.stop)

class IonIntegrationTestCase(unittest.TestCase):
    """
    Base test class to allow operations such as starting the container
    TODO: Integrate with IonUnitTestCase
    """

    # @see
    # http://www.saltycrane.com/blog/2012/07/how-prevent-nose-unittest-using-docstring-when-verbosity-2/
    def shortDescription(self):
        return None

    # override __str__ and __repr__ behavior to show a copy-pastable nosetest name for ion tests
    #  ion.module:TestClassName.test_function_name
    def __repr__(self):
        name = self.id()
        name = name.split('.')
        if name[0] not in ["ion", "pyon"]:
            return "%s (%s)" % (name[-1], '.'.join(name[:-1]))
        else:
            return "%s ( %s )" % (name[-1], '.'.join(name[:-2]) + ":" + '.'.join(name[-2:]))
    __str__ = __repr__


    def run(self, result=None):
        unittest.TestCase.run(self, result)

    def _start_container(self):
        # hack to force queue auto delete on for int tests
        self._turn_on_queue_auto_delete()
        self._patch_out_diediedie()
        self._patch_out_fail_fast_kill()

        bootstrap.testing_fast = True

        if os.environ.get('CEI_LAUNCH_TEST', None):
            # Let's force clean again.  The static initializer is causing
            # issues
            #self._force_clean()
            self._patch_out_start_rel()
            from pyon.datastore.datastore_admin import DatastoreAdmin
            from pyon.datastore.datastore_common import DatastoreFactory
            da = DatastoreAdmin(config=CFG)
            da.load_datastore('res/dd')
            # Turn off file system cleaning
            # The child container should NOT clean out the parent's filesystem,
            # they should share like good containers sometimes do
            CFG.container.file_system.force_clean = False
        else:
            # We cannot live without pre-initialized datastores and resource objects
            pre_initialize_ion()

            # hack to force_clean on filesystem
            try:
                CFG['container']['filesystem']['force_clean'] = True
            except KeyError:
                CFG['container']['filesystem'] = {}
                CFG['container']['filesystem']['force_clean'] = True

        self.container = None
        self.addCleanup(self._stop_container)
        self.container = Container()
        self.container.start()

        bootstrap.testing_fast = False


    def _stop_container(self):
        bootstrap.testing_fast = True
        if self.container:
            self.container.stop()
            self.container = None
        self._force_clean()         # deletes only
        bootstrap.testing_fast = False

    def _start_tracer_log(self, config=None):
        """Temporarily enables tracer log and configures it until end of test (cleanUp)"""
        if not self.container:
            return
        from pyon.util import tracer
        if not tracer.trace_data["config"].get("log_trace", False):
            tracer_cfg_old = tracer.trace_data["config"]
            tracer.trace_data["config"] = tracer.trace_data["config"].copy()
            tracer.trace_data["config"]["log_trace"] = True
            if config:
                tracer.trace_data["config"].update(config)

            def cleanup_tracer():
                tracer.trace_data["config"] = tracer_cfg_old
                log.info("--------------- Stopping Tracer Logging ---------------")
            self.addCleanup(cleanup_tracer)
            log.info("--------------- Starting Tracer Logging ---------------")

    def _breakpoint(self, scope=None, global_scope=None):
        from pyon.util.breakpoint import breakpoint
        breakpoint(scope=scope, global_scope=global_scope)

    def _turn_on_queue_auto_delete(self):
        patcher = patch('pyon.net.channel.RecvChannel._queue_auto_delete', True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _patch_out_diediedie(self):
        """
        If things are running slowly, diediedie will send a kill -9 to the owning process,
        which could be the test runner! Let the test runner decide if it's time to die.
        """
        patcher = patch('pyon.core.thread.shutdown_or_die')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _patch_out_start_rel(self):
        def start_rel_from_url(*args, **kwargs):
            return True

        patcher = patch('pyon.container.apps.AppManager.start_rel_from_url', start_rel_from_url)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _patch_out_fail_fast_kill(self):

        patcher = patch('pyon.container.cc.Container._kill_fast')
        patcher.start()
        self.addCleanup(patcher.stop)

    @classmethod
    def _force_clean(cls, recreate=False):
        from pyon.core.bootstrap import get_sys_name, CFG
        from pyon.datastore.datastore_common import DatastoreFactory
        datastore = DatastoreFactory.get_datastore(config=CFG, variant=DatastoreFactory.DS_BASE, scope=get_sys_name())
        #datastore = DatastoreFactory.get_datastore(config=CFG, variant=DatastoreFactory.DS_BASE)

        dbs = datastore.list_datastores()
        things_to_clean = filter(lambda x: x.startswith('%s_' % get_sys_name().lower()), dbs)
        try:
            for thing in things_to_clean:
                datastore.delete_datastore(datastore_name=thing)
                if recreate:
                    datastore.create_datastore(datastore_name=thing)

        finally:
            datastore.close()

        if os.environ.get('CEI_LAUNCH_TEST', None) is None:
            FileSystem._clean(CFG)


    def patch_cfg(self, cfg_obj_or_str, *args, **kwargs):
        """
        Helper method for patching the CFG (or any dict, but useful for patching CFG).

        This method exists because the decorator versions of patch/patch.dict do not function
        until the test_ method is called - ie, when setUp is run, the patch hasn't occured yet.
        Use this in your setUp method if you need to patch CFG and have stuff in setUp respect it.

        @param  cfg_obj_or_str  An actual ref to CFG or a string defining where to find it ie 'pyon.ion.exchange.CFG'
        @param  *args           *args to pass to patch.dict
        @param  **kwargs        **kwargs to pass to patch.dict
        """
        patcher = patch.dict(cfg_obj_or_str, *args, **kwargs)
        patcher.start()
        self.addCleanup(patcher.stop)



class MiUnitTest(unittest.TestCase):
    """
    Base class for non-ion tests.  Use only if needed to avoid ion 
    test common code.
    """
    def shortDescription(self):
        return None


class IonUnitTestCase(unittest.TestCase):
    pass


class MiUnitTestCase(IonUnitTestCase):
    """
    Base class for most tests in MI.
    """
    pass

class MiTestCase(PyonTestCase):
    """
    Base class for most tests in MI.
    """
    def shortDescription(self):
        return None

    def test_verify_service(self):
        pass

class MiIntTestCase(IonIntegrationTestCase):
    """
    Base class for most tests in MI.
    """

    def shortDescription(self):
        return None

class ParticleTestMixin(object):
    """
    A class with some methods to test data particles. Intended to be mixed
    into test classes so that particles can be tested in different areas of
    the MI code base.
    """

    def convert_data_particle_to_dict(self, data_particle):
        """
        Convert a data particle object to a dict.  This will work for data
        particles as DataParticle object, dictionaries or a string
        @param data_particle data particle
        @return dictionary representation of a data particle
        """
        if (isinstance(data_particle, DataParticle)):
            sample_dict = data_particle.generate_dict()
        elif (isinstance(data_particle, str)):
            sample_dict = json.loads(data_particle)
        elif (isinstance(data_particle, dict)):
            sample_dict = data_particle
        else:
            raise IDKException("invalid data particle type: %s", type(data_particle))

        return sample_dict

    def get_data_particle_values_as_dict(self, data_particle):
        """
        Return all of the data particle values as a dictionary with the value
        id as the key and the value as the value.  This method will decimate
        the data, in the any characteristics other than value id and value.
        i.e. binary.
        @param data_particle data particle to inspect
        @return return a dictionary with keys and values { value-id: value }
        @throws IDKException when missing values dictionary
        """
        sample_dict = self.convert_data_particle_to_dict(data_particle)

        values = sample_dict.get('values')
        if(not values):
            raise IDKException("Data particle missing values")

        if(not isinstance(values, list)):
            raise IDKException("Data particle values not a list")

        result = {}
        for param in values:
            if(not isinstance(param, dict)):
                raise IDKException("must be a dict")

            key = param.get('value_id')
            if(key == None):
                raise IDKException("value_id not defined")

            if(key in result.keys()):
                raise IDKException("duplicate value detected for %s" % key)

            result[key] = param.get('value')

        return result

    def assert_data_particle_keys(self, data_particle_key, test_config):
        """
        Ensure that the keys defined in the data particle key enum match
        the keys defined in the test configuration.
        @param data_particle_key object that defines all data particle keys.
        @param test_config dictionary containing parameter verification values
        """
        driver_keys = sorted(data_particle_key.list())
        test_config_keys = sorted(test_config.keys())

        self.assertEqual(driver_keys, test_config_keys)

    def assert_data_particle_header(self, data_particle, stream_name, require_instrument_timestamp=False):
        """
        Verify a data particle header is formatted properly
        @param data_particle version 1 data particle
        @param stream_name version 1 data particle
        @param require_instrument_timestamp should we verify the instrument timestamp exists
        """
        sample_dict = self.convert_data_particle_to_dict(data_particle)
        log.debug("SAMPLEDICT: %s", sample_dict)

        self.assertTrue(sample_dict[DataParticleKey.STREAM_NAME], stream_name)
        self.assertTrue(sample_dict[DataParticleKey.PKT_FORMAT_ID], DataParticleValue.JSON_DATA)
        self.assertTrue(sample_dict[DataParticleKey.PKT_VERSION], 1)
        self.assertIsInstance(sample_dict[DataParticleKey.VALUES], list)

        self.assertTrue(sample_dict.get(DataParticleKey.PREFERRED_TIMESTAMP))

        self.assertIsNotNone(sample_dict.get(DataParticleKey.DRIVER_TIMESTAMP))
        self.assertIsInstance(sample_dict.get(DataParticleKey.DRIVER_TIMESTAMP), float)

        # It is highly unlikely that we should have a particle without a port agent timestamp,
        # at least that's the current assumption.
        self.assertIsNotNone(sample_dict.get(DataParticleKey.PORT_TIMESTAMP))
        self.assertIsInstance(sample_dict.get(DataParticleKey.PORT_TIMESTAMP), float)

        if(require_instrument_timestamp):
            self.assertIsNotNone(sample_dict.get(DataParticleKey.INTERNAL_TIMESTAMP))
            self.assertIsInstance(sample_dict.get(DataParticleKey.INTERNAL_TIMESTAMP), float)



