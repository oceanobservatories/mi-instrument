"""
Usage:
    main.py <module> <driver_class> <refdes> <event_url> <particle_url>
    main.py datalog <module> <protocol_class> <event_url> <particle_url> <files>...
    main.py digidatalogascii <module> <protocol_class> <event_url> <particle_url> <files>...
    main.py chunkdatalog <module> <protocol_class> <event_url> <particle_url> <files>...

Options:
    -h, --help          Show this screen.

"""

from docopt import docopt
from mi.core.instrument.wrapper import DriverWrapper
from mi.core.instrument.playback import PlaybackWrapper, DatalogReader, DigiDatalogAsciiReader
from mi.core.instrument.playback import ChunkyDatalogReader

options = docopt(__doc__)

module = options['<module>']
event_url = options['<event_url>']
particle_url = options['<particle_url>']
klass = options.get('<protocol_class>')
files = options.get('<files>')

if options['datalog']:
    dp = PlaybackWrapper(
        module, klass, None, event_url, particle_url, DatalogReader, files)
elif options['digidatalogascii']:
    dp = PlaybackWrapper(
        module, klass, None, event_url, particle_url, DigiDatalogAsciiReader, files)
elif options['chunkdatalog']:
    dp = PlaybackWrapper(
        module, klass, None, event_url, particle_url, ChunkyDatalogReader, files)
else:
    klass = options['<driver_class>']
    refdes = options['<refdes>']
    dp = DriverWrapper(module, klass, refdes, event_url, particle_url)

dp.run()
