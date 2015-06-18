import sys
from mi.core.instrument.wrapper import ZmqDriverProcess

if __name__ == '__main__':
    module = sys.argv[1]
    klass = sys.argv[2]
    command_port = int(sys.argv[3])
    event_port = int(sys.argv[4])
    dp = ZmqDriverProcess(module, klass, command_port, event_port)
    dp.run()
