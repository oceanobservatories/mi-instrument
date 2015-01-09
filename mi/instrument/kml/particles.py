"""
@package mi.instrument.KML.particles
@file marine-integrations/mi/instrument/KML/driver.py
@author Sung Ahn
@brief Driver particle code for the KML particles
Release notes:
"""
import struct
import re
from mi.instrument.kml.driver import KMLParameter

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import SampleException

from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, RawDataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType

from mi.core.instrument.protocol_param_dict import ProtocolParameterDict

from mi.instrument.kml.driver import ParameterIndex

#
# Particle Regex's'
#
CAMDS_DISK_STATUS_MATCHER = r'<\x0B:\x06:GC.+?>'
CAMDS_DISK_STATUS_MATCHER_COM = re.compile(CAMDS_DISK_STATUS_MATCHER, re.DOTALL)
CAMDS_HEALTH_STATUS_MATCHER = r'<\x07:\x06:HS.+?>'
CAMDS_HEALTH_STATUS_MATCHER_COM = re.compile(CAMDS_HEALTH_STATUS_MATCHER, re.DOTALL)
CAMDS_SNAPSHOT_MATCHER = r'<\x04:\x06:CI>'
CAMDS_SNAPSHOT_MATCHER_COM = re.compile(CAMDS_SNAPSHOT_MATCHER, re.DOTALL)
CAMDS_START_CAPTURING = r'<\x04:\x06:SP>'
CAMDS_START_CAPTURING_COM = re.compile(CAMDS_START_CAPTURING, re.DOTALL)
CAMDS_STOP_CAPTURING = r'<\x04:\x06:SR>'
CAMDS_STOP_CAPTURING_COM = re.compile(CAMDS_STOP_CAPTURING, re.DOTALL)


# ##############################################################################
# Data Particles
# ##############################################################################
class DataParticleType(BaseEnum):
    """
    Stream types of data particles
    """
    RAW = CommonDataParticleType.RAW

    CAMDS_VIDEO = "camds_video"
    CAMDS_HEALTH_STATUS = "camds_health_status"
    CAMDS_DISK_STATUS = "camds_disk_status"
    CAMDS_IMAGE_METADATA = "camds_image_metadata"

# keys for video stream
class CAMDS_VIDEO_KEY(BaseEnum):
    """
    Video stream data key
    """
    CAMDS_VIDEO_BINARY = "raw"

# Data particle for PT4 command
class CAMDS_VIDEO(RawDataParticle):
    """
    cam video stream data particle
    """
    _data_particle_type = DataParticleType.CAMDS_VIDEO

# HS command
class CAMDS_HEALTH_STATUS_KEY(BaseEnum):
    """
    cam health status keys
    """
    temp = "camds_temp"
    humidity = "camds_humidity"
    error = "camds_error"

# Data particle for HS command
class CAMDS_HEALTH_STATUS(DataParticle):
    """
    cam health status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_HEALTH_STATUS

    def build_data_particle(self, temp = None, humidity = None,
                            error = None):
        result = []
        for key in [CAMDS_HEALTH_STATUS_KEY.temp,
                    CAMDS_HEALTH_STATUS_KEY.humidity,
                    CAMDS_HEALTH_STATUS_KEY.error ]:

            if key == CAMDS_HEALTH_STATUS_KEY.temp:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : temp
                })

            if key == CAMDS_HEALTH_STATUS_KEY.humidity:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : humidity
                })

            if key == CAMDS_HEALTH_STATUS_KEY.error:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : error
                })
        return result

    def _build_parsed_values(self):

        response_striped = self.raw_data

        #check the size of the response
        if len(response_striped) != 11:
            log.error("Health status size should be 11 %r" + response_striped)
            return
        if response_striped[0] != '<':
            log.error("Health status is not correctly formatted %r" + response_striped)
            return
        if response_striped[len(response_striped) -1] != '>':
            log.error("Health status is not correctly formatted %r" + response_striped)
            return

        int_bytes = bytearray(response_striped)
        _temp = int_bytes[7]
        _humidity = int_bytes[8]
        _error = int_bytes[9]

        sample = CAMDS_HEALTH_STATUS(response_striped)
        parsed_sample = sample.build_data_particle(temp = _temp, humidity = _humidity,
                                                   error = _error)

        log.debug("CAMDS_HEALTH_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


# GC command
class CAMDS_DISK_STATUS_KEY(BaseEnum):
    """
    cam disk status keys
    """
    size = "camds_disk_size"
    disk_remaining = "camds_disk_remaining"
    image_remaining = "camds_images_remaining"
    image_on_disk = "camds_images_on_disk"

# Data particle for GC command
class CAMDS_DISK_STATUS(DataParticle):
    """
    cam disk status data particle
    """
    _data_particle_type = DataParticleType.CAMDS_DISK_STATUS

    def build_data_particle(self, size = None, disk_remaining = None,
                            image_remaining = None, image_on_disk = None):
        result = []
        for key in [CAMDS_DISK_STATUS_KEY.size,
                    CAMDS_DISK_STATUS_KEY.disk_remaining,
                    CAMDS_DISK_STATUS_KEY.image_remaining,
                    CAMDS_DISK_STATUS_KEY.image_on_disk]:

            if key == CAMDS_DISK_STATUS_KEY.size:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : size
                })

            if key == CAMDS_DISK_STATUS_KEY.disk_remaining:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : disk_remaining
                })

            if key == CAMDS_DISK_STATUS_KEY.image_remaining:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : image_remaining
                })

            if key == CAMDS_DISK_STATUS_KEY.image_on_disk:
                result.append( {
                DataParticleKey.VALUE_ID: key,
                DataParticleKey.VALUE : image_on_disk
                })
        return result

    def _build_parsed_values(self):

        response_striped = self.raw_data.strip()

        #check the size of the response
        if len(response_striped) != 15:
            log.error("Disk status size should be 15 %r" + response_striped)
            return
        if response_striped[0] != '<':
            log.error("Disk status is not correctly formatted %r" + response_striped)
            return
        if response_striped[len(response_striped) -1] != '>':
            log.error("Disk status is not correctly formatted %r" + response_striped)
            return

        int_bytes = bytearray(response_striped)

        byte1 = int_bytes[7]
        byte2 = int_bytes[8]
        byte3 = int_bytes[9]

        available_disk = byte1 * pow(10, byte2)
        available_disk_percent = byte3

        temp = struct.unpack('!h', response_striped[10] + response_striped[11])
        images_remaining = temp[0]
        temp = struct.unpack('!h', response_striped[12] + response_striped[13])
        images_on_disk = temp[0]

        sample = CAMDS_DISK_STATUS(response_striped)
        parsed_sample = sample.build_data_particle(size = available_disk, disk_remaining = available_disk_percent,
                            image_remaining = images_remaining,
                            image_on_disk = images_on_disk)

        log.debug("CAMDS_DISK_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


#cam meta data particle
class CAMDS_IMAGE_METADATA_KEY(BaseEnum):
    """
    cam image meta data keys
    """
    PAN_POSITION = "camds_pan_position"
    TILT_POSITION = "camds_tilt_position"
    FOCUS_POSITION = "camds_focus_position"
    ZOOM_POSITION = "camds_zoom_position"
    IRIS_POSITION = "camds_iris_position"
    GAIN = "camds_gain"
    RESOLUTION = "camds_resolution"
    BRIGHTNESS = "camds_brightness"

# Data particle for CAMDS Image Metadata
class CAMDS_IMAGE_METADATA(DataParticle):
    """
    cam image data particle
    """
    _data_particle_type = DataParticleType.CAMDS_IMAGE_METADATA

    def _build_parsed_values(self):
        # Initialize

        result = []

        log.debug("CAMDS_IMAGE_METADATA: Building data particle...")

        param_dict = self.raw_data.get_all()

        log.debug("Param Dict: %s" % param_dict)

        for key, value in param_dict.items():

            if key == KMLParameter.PAN_POSITION[ParameterIndex.KEY]:
                log.error("PAN_POSITION")
                result.append({DataParticleKey.VALUE_ID: "camds_pan_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.PAN_POSITION[ParameterIndex.KEY])})
            elif key == KMLParameter.TILT_POSITION[ParameterIndex.KEY]:
                log.error("TILT_POSITION")
                result.append({DataParticleKey.VALUE_ID: "camds_tilt_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.TILT_POSITION[ParameterIndex.KEY])})
            elif key == KMLParameter.FOCUS_POSITION[ParameterIndex.KEY]:
                log.error("FOCUS_POSITION")
                result.append({DataParticleKey.VALUE_ID: "camds_focus_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.FOCUS_POSITION[ParameterIndex.KEY])})
            elif key == KMLParameter.ZOOM_POSITION[ParameterIndex.KEY]:
                log.error("ZOOM_POSITION")
                result.append({DataParticleKey.VALUE_ID: "camds_zoom_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.ZOOM_POSITION[ParameterIndex.KEY])})
            elif key == KMLParameter.IRIS_POSITION[ParameterIndex.KEY]:
                log.error("IRIS_POSITION")
                result.append({DataParticleKey.VALUE_ID: "camds_iris_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.IRIS_POSITION[ParameterIndex.KEY])})
            elif key == KMLParameter.CAMERA_GAIN[ParameterIndex.KEY]:
                #TODO: Instrument has issues returning CAMERA_GAIN, set to default value for now
                # log.error("CAMERA_GAIN")
                # result.append({DataParticleKey.VALUE_ID: "camds_gain",
                #            DataParticleKey.VALUE: param_dict.get(KMLParameter.CAMERA_GAIN[ParameterIndex.KEY])})
                gain_val = 255
                result.append({DataParticleKey.VALUE_ID: "camds_gain",
                           DataParticleKey.VALUE: gain_val})
            elif key == KMLParameter.IMAGE_RESOLUTION[ParameterIndex.KEY]:
                log.error("IMAGE_RESOLUTION")
                result.append({DataParticleKey.VALUE_ID: "camds_resolution",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.IMAGE_RESOLUTION[ParameterIndex.KEY])})
            elif key == KMLParameter.LAMP_BRIGHTNESS[ParameterIndex.KEY]:
                log.error("LAMP_BRIGHTNESS")
                brightness_param = param_dict.get(KMLParameter.LAMP_BRIGHTNESS[ParameterIndex.KEY])

                try:
                    brightness = int(brightness_param.split(':')[1])
                    result.append({DataParticleKey.VALUE_ID: "camds_brightness",
                               DataParticleKey.VALUE: brightness})

                except ValueError:
                    raise SampleException("ValueError while setting lamp brightness: [%s]" %
                                  brightness_param)

                except IndexError:
                    raise SampleException("IndexError while setting lamp brightness: [%s]" %
                                  brightness_param)

        log.debug("CAMDS_IMAGE_METADATA: Finished building particle: %s" % result)

        return result