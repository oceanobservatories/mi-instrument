#!/usr/bin/env python
"""
@brief Web-based server, servicing Alerts & Alarms events posted by the
OMS.  This server will register itself as a listener to the OMS Server.
It will publish OMS Events containing Alert & Alarm messages to the
qpid queue specified in the configuration file:
oms_alert_alarm_server.cfg.

Usage:
    oms_alert_alarm_server <server_config>

Options:
    -h, --help          Show this screen.

"""

import httplib
import xmlrpclib

import re
import yaml
from docopt import docopt
from flask import Flask, request
from mi.core.instrument.publisher import Publisher
from mi.core.log import LoggerManager
from mi.logging import log

app = Flask(__name__)
aa_publisher = None

LoggerManager()
log.setLevel('INFO')


@app.route('/', methods=['POST'])
def process_oms_request():
    """
    This is the method that is called when the OMS POSTs OMS Events to
    this registered listener at the "/" path.
    :return:
    """

    if isinstance(request.json, list):
        # Log the list of Alert & Alarm messages from the OMS Event
        for alert_alarm_dict in request.json:
            aa_publisher.enqueue(alert_alarm_dict)
            log.info('oms_alert_alarm_server: OMS_AA_MSG: %r', alert_alarm_dict)

        # Publish the list of Alert & Alarm messages to qpid
        aa_publisher.publish()

    else:
        log.error('No data in the POSTed alert/alarm OMS Event ...')

    return '', httplib.ACCEPTED


def start_web_service(oms_uri, alert_alarm_server_uri):
    """
    This method gets the proxy for the OMS Server, registers this server
    as a listener to the OMS and starts the Flask web service that will
    listen for OMS Events from the OMS Server.
    :param oms_uri: The URI of the OMS Server
    :param alert_alarm_server_uri: The URI of this server.
    :return:
    """

    alert_alarm_server_port = int(re.search('http://.+:(.+?)/', alert_alarm_server_uri).group(1))

    if oms_uri == 'DEBUG':
        log.info('DEBUG mode: OMS Alert Alarm Server not registering with OMS.')
    else:
        log.info('Getting the proxy for OMS server: %r', oms_uri)
        proxy = xmlrpclib.ServerProxy(oms_uri)

        log.info('Registering OMS Alerts & Alarms server as listener: %r', alert_alarm_server_uri)
        proxy.event.register_event_listener(alert_alarm_server_uri)

    log.info('Listening for Alerts & Alarms on 0.0.0.0:%d', alert_alarm_server_port)
    app.run(host='0.0.0.0', port=alert_alarm_server_port)


def main():
    """
    This main routine will get the configuration file from the command
    line parameter and set the values for required URIs for the OMS, the
    OMS Alert Alarm Server and the qpid Server.  It will then get the qpid
    publisher for publishing the OMS events.  Finally, it will start the web
    service.
    """

    global aa_publisher

    options = docopt(__doc__)
    server_config_file = options['<server_config>']
    try:
        config = yaml.load(open(server_config_file))
    except IOError:
        log.error('Cannot find configuration file: %r', server_config_file)
        return

    try:
        oms_uri = config.get('oms_uri')
        alert_alarm_server_uri = config.get('alert_alarm_server_uri')
        qpid_uri = config.get('qpid_uri')
    except AttributeError:
        log.error('Configuration file is empty: %r', server_config_file)
        return

    if not all((oms_uri, alert_alarm_server_uri, qpid_uri)):
        log.error('Missing mandatory configuration values missing from %r', server_config_file)
    else:
        headers = {'aaServerUri': alert_alarm_server_uri}

        try:
            aa_publisher = Publisher.from_url(qpid_uri, headers=headers)
            start_web_service(oms_uri, alert_alarm_server_uri)

        except Exception as ex:
            log.exception('Error starting OMS Alert and Alarm web service: %r', ex)
            return


if __name__ == '__main__':
    main()
    log.info('Stopping OMS Alert & Alarm Web Service.\n')
