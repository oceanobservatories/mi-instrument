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

from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, RawDataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType

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

    TEMP_INDEX = 7
    HUMIDITY_INDEX = 8
    ERROR_INDEX = 9


    def _build_parsed_values(self):

        response_stripped = self.raw_data

        #check the size of the response
        if len(response_stripped) != 11:
            log.error("Health status size should be 11 %r" + response_stripped)
            return
        if response_stripped[0] != '<':
            log.error("Health status is not correctly formatted %r" + response_stripped)
            return
        if response_stripped[-1] != '>':
            log.error("Health status is not correctly formatted %r" + response_stripped)
            return

        int_bytes = bytearray(response_stripped)

        parsed_sample = [{DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.temp,
                          DataParticleKey.VALUE: int_bytes[self.TEMP_INDEX]},
                         {DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.humidity,
                          DataParticleKey.VALUE: int_bytes[self.HUMIDITY_INDEX]},
                         {DataParticleKey.VALUE_ID: CAMDS_HEALTH_STATUS_KEY.error,
                          DataParticleKey.VALUE: int_bytes[self.ERROR_INDEX]}]

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

    BYTE_1_INDEX = 7
    BYTE_2_INDEX = 8
    BYTE_3_INDEX = 9

    def _build_parsed_values(self):

        response_stripped = self.raw_data.strip()

        #check the size of the response
        if len(response_stripped) != 15:
            log.error("Disk status size should be 15 %r" + response_stripped)
            return
        if response_stripped[0] != '<':
            log.error("Disk status is not correctly formatted %r" + response_stripped)
            return
        if response_stripped[-1] != '>':
            log.error("Disk status is not correctly formatted %r" + response_stripped)
            return

        int_bytes = bytearray(response_stripped)

        byte1 = int_bytes[self.BYTE_1_INDEX]
        byte2 = int_bytes[self.BYTE_2_INDEX]
        byte3 = int_bytes[self.BYTE_3_INDEX]

        available_disk = byte1 * pow(10, byte2)
        available_disk_percent = byte3

        temp = struct.unpack('!h', response_stripped[10] + response_stripped[11])
        images_remaining = temp[0]
        temp = struct.unpack('!h', response_stripped[12] + response_stripped[13])
        images_on_disk = temp[0]

        parsed_sample = [{DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.size,
                      DataParticleKey.VALUE: available_disk},
                     {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.disk_remaining,
                      DataParticleKey.VALUE: available_disk_percent},
                     {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.image_remaining,
                      DataParticleKey.VALUE: images_remaining},
                     {DataParticleKey.VALUE_ID: CAMDS_DISK_STATUS_KEY.image_on_disk,
                      DataParticleKey.VALUE: images_on_disk}]

        log.debug("CAMDS_DISK_STATUS: Finished building particle: %s" % parsed_sample)

        return parsed_sample


# Data particle for CAMDS Image Metadata
class CAMDS_IMAGE_METADATA(DataParticle):
    """
    camds_image_metadata particle
    """
    _data_particle_type = DataParticleType.CAMDS_IMAGE_METADATA

    def _build_parsed_values(self):
        # Initialize

        result = []

        log.debug("CAMDS_IMAGE_METADATA: Building data particle...")

        param_dict = self.raw_data.get_all()

        log.debug("Param Dict: %s" % param_dict)

        result.append({DataParticleKey.VALUE_ID: "camds_pan_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.PAN_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_tilt_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.TILT_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_focus_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.FOCUS_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_zoom_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.ZOOM_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_iris_position",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.IRIS_POSITION[ParameterIndex.KEY])})
        result.append({DataParticleKey.VALUE_ID: "camds_resolution",
                           DataParticleKey.VALUE: param_dict.get(KMLParameter.IMAGE_RESOLUTION[ParameterIndex.KEY])})

        brightness_param = param_dict.get(KMLParameter.LAMP_BRIGHTNESS[ParameterIndex.KEY])

        try:
            brightness = int(brightness_param.split(':')[1])
            result.append({DataParticleKey.VALUE_ID: "camds_brightness", DataParticleKey.VALUE: brightness})

        except ValueError:
            log.error("Error building camds_image_metadata particle: Brightness value is not an Integer")

        except IndexError:
            log.error("Error building camds_image_metadata particle: Brightness parameter incorrectly "
                      "formatted: %s" % brightness_param)

        #TODO: Instrument has issues returning CAMERA_GAIN, set to default value for now
        # result.append({DataParticleKey.VALUE_ID: "camds_gain",
        #            DataParticleKey.VALUE: param_dict.get(KMLParameter.CAMERA_GAIN[ParameterIndex.KEY])})
        gain_val = 255
        result.append({DataParticleKey.VALUE_ID: "camds_gain", DataParticleKey.VALUE: gain_val})

        log.debug("CAMDS_IMAGE_METADATA: Finished building particle: %s" % result)

        return result