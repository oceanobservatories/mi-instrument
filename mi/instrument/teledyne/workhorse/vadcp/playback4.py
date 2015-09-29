from mi.instrument.teledyne.workhorse.vadcp.driver import Protocol, SlaveProtocol

__author__ = 'petercable'


class PlaybackProtocol(Protocol):
    def __init__(self, driver_event):
        super(PlaybackProtocol, self).__init__(None, None, driver_event, [SlaveProtocol.FOURBEAM])

    def got_data(self, port_agent_packet, connection=SlaveProtocol.FOURBEAM):
        super(PlaybackProtocol, self).got_data(port_agent_packet, connection)


def create_playback_protocol(callback):
    return PlaybackProtocol(callback)
