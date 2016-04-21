#!/usr/bin/env python
"""
@brief Web-based server, servicing Alerts & Alarms events posted by the OMS.  This server will
register itself as a listener to the OMS Server.  It will publish OMS Events containing Alert
& Alarm messages to the qpid queue specified in the configuration file: oms_alert_alarm_server.cfg.

Usage:
    oms_alert_alarm_server
    oms_alert_alarm_server <server_config>

Options:
    -h, --help          Show this screen.

"""

import json
import xmlrpclib
import yaml
from docopt import docopt
from flask import Flask, request
from mi.core.instrument.publisher import Publisher
from mi.core.log import LoggerManager
from ooi.logging import log

LoggerManager()
app = Flask(__name__)
aa_publisher = None

OMS_IP = '10.3.1.10'
AA_SERVER_IP = '192.168.150.103'
AA_SERVER_PORT = 12590
AA_QPID_SERVER = 'ooiufs03.ooi.rutgers.edu'


@app.route('/', methods=['POST'])
def process_oms_request():
    """
    This is the method that is called when the OMS POSTs OMS Events to this registered listener
    at the "/" path.
    :return:
    """
    if len(request.data) != 0:
        # Get the list of Alert & Alarm messages from the OMS Event
        alert_alarm_list = json.loads(request.data)

        # Log the list of Alert & Alarm messages from the OMS Event
        for alert_alarm_dict in alert_alarm_list:
            log_str = 'oms_alert_alarm_server: POST:'
            for aa_item_key, aa_item_val in alert_alarm_dict.iteritems():
                log_str += '\n\t' + str(aa_item_key) + ' : ' + str(aa_item_val)
            log.info(log_str)

        # Publish the list of Alert & Alarm messages to qpid
        aa_publisher.publish(alert_alarm_list)

    else:
        log.error('No data in the POSTed alert/alarm OMS Event ...')

    return ''


def start_web_service(oms_ip, alert_alarm_server_ip, alert_alarm_server_port):
    """
    This method get the proxy for the OMS Server, registers this server as a listener to the OMS and starts
    the Flask web service, listening for OMS Events from the OMS Server.
    :param oms_ip: The IP Address of the OMS Server
    :param alert_alarm_server_ip: The IP address on which this server is running.
    :param alert_alarm_server_port: The port on which this server listens to for OMS POSTing.
    :return:
    """
    oms_server_url = 'http://alice:1234@' + oms_ip + ':9021'
    oms_alert_alarm_server_url = 'http://' + alert_alarm_server_ip + ':' + str(alert_alarm_server_port) + '/'

    if oms_ip == 'DEBUG':
        print 'DEBUG Mode: OMS Alert Alarm Server not registering with OMS.'
    else:
        print ' Getting the proxy for OMS server: ' + oms_server_url
        proxy = xmlrpclib.ServerProxy(oms_server_url)

        print ' Registering OMS Alerts & Alarms server as listener: ' + oms_alert_alarm_server_url
        proxy.event.register_event_listener(oms_alert_alarm_server_url)

    print ' Listening for Alerts & Alarms on ' + oms_alert_alarm_server_url
    app.run(host='0.0.0.0', port=alert_alarm_server_port)


def main():
    """
    This main routine will get the config file form the command line parameter and set the values for the configuration
    paramters or use the default values if no config file is entered.  I will then get the qpid publisher and start
    the web service.
    :return:
    """
    global aa_publisher

    oms_ip = OMS_IP
    alert_alarm_server_ip = AA_SERVER_IP
    alert_alarm_server_port = AA_SERVER_PORT
    qpid_server = AA_QPID_SERVER

    options = docopt(__doc__)
    if options['<server_config>'] is not None:
        config = yaml.load(open(options['<server_config>']))

        if config['oms_ip'] is not None:
            oms_ip = config['oms_ip']

        if config['alert_alarm_server_ip'] is not None:
            alert_alarm_server_ip = config['alert_alarm_server_ip']

        if config['alert_alarm_server_port'] is not None:
            alert_alarm_server_port = int(config['alert_alarm_server_port'])

        if config['qpid_server'] is not None:
            qpid_server = config['qpid_server']

    qpid_url = 'qpid://guest/guest@' + qpid_server + '?queue=Ingest.instrument_oms_events'
    headers = {'omsServerIp': alert_alarm_server_ip}

    try:
        aa_publisher = Publisher.from_url(qpid_url, headers)
        start_web_service(oms_ip, alert_alarm_server_ip, alert_alarm_server_port)

    except Exception as ex:
        log.exception('Error starting OMS Alert and Alarm web service: %r', ex)

    print ' Stopping OMS Alert & Alarm Web Service.\n'

if __name__ == '__main__':
    main()
