import time
from mi.core.common import InstErrorCode
from mi.core.instrument.data_particle import RawDataParticle
from mi.core.instrument.instrument_driver import DriverProtocolState, DriverAsyncEvent

from mi.core.log import get_logger

log = get_logger()

from mi.core.exceptions import InstrumentProtocolException, InstrumentTimeoutException
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, RE_PATTERN, DEFAULT_CMD_TIMEOUT, \
    DEFAULT_WRITE_DELAY, MAX_BUFFER_SIZE


class MultiInstrumentProtocol(CommandResponseInstrumentProtocol):
    """
    Base class for text-based command-response instruments.
    """

    def __init__(self, prompts, newline, driver_event, connections=None):
        """
        Constructor.
        @param prompts Enum class containing possible device prompts used for
        command response logic.
        @param newline The device newline.
        @driver_event The callback for asynchronous driver events.
        """
        if not type(connections) is list:
            raise InstrumentProtocolException('Unable to instantiate multi connection protocol without connection list')

        # Construct superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # The end of line delimiter.
        self._newline = newline

        # Class of prompts used by device.
        self._prompts = prompts

        self._linebuf = {connection: '' for connection in connections}
        self._promptbuf = {connection: '' for connection in connections}

        # Handlers to build commands.
        self._build_handlers = {}

        # Handlers to parse responses.
        self._response_handlers = {connection: '' for connection in connections}

        self._last_data_receive_timestamp = None

        self.connections = {connection: None for connection in connections}
        self.chunkers = {connection: None for connection in connections}

    def _get_response(self, timeout=10, expected_prompt=None, response_regex=None, connection=None):
        """
        Get a response from the instrument, but be a bit loose with what we
        find. Leave some room for white space around prompts and not try to
        match that just in case we are off by a little whitespace or not quite
        at the end of a line.

        @todo Consider cases with no prompt
        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @param response_regex Look for a response value that matches the
        supplied compiled regex pattern. Groups that match will be returned as a
        string. Cannot be used with expected prompt. None
        will be returned as a prompt with this match. If a regex is supplied,
        internal the prompt list will be ignored.
        @retval Regex search result tuple (as MatchObject.groups() would return
        if a response_regex is supplied. A tuple of (prompt, response) if a
        prompt is looked for.
        @throw InstrumentProtocolException if both regex and expected prompt are
        passed in or regex is not a compiled pattern.
        @throw InstrumentTimeoutException on timeout
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')
        # Grab time for timeout and wait for prompt.

        starttime = time.time()

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        if expected_prompt is None:
            prompt_list = self._get_prompts()
        else:
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        if response_regex is None:
            pattern = None
        else:
            pattern = response_regex.pattern

        log.debug('_get_response: timeout=%s, prompt_list=%s, expected_prompt=%r, response_regex=%r, promptbuf=%r',
                  timeout, prompt_list, expected_prompt, pattern, self._promptbuf)
        while True:
            if response_regex:
                # noinspection PyArgumentList
                match = response_regex.search(self._linebuf[connection])
                if match:
                    return match.groups()
            else:
                for item in prompt_list:
                    index = self._promptbuf[connection].find(item)
                    if index >= 0:
                        result = self._promptbuf[connection][0:index + len(item)]
                        return item, result

            time.sleep(.1)

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in InstrumentProtocol._get_response()")

    def _get_raw_response(self, timeout=10, expected_prompt=None, connection=None):
        """
        Get a response from the instrument, but don't trim whitespace. Used in
        times when the whitespace is what we are looking for.

        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @throw InstrumentProtocolException on timeout
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        # Grab time for timeout and wait for prompt.
        strip_chars = "\t "

        starttime = time.time()
        if expected_prompt is None:
            prompt_list = self._get_prompts()
        else:
            if isinstance(expected_prompt, str):
                prompt_list = [expected_prompt]
            else:
                prompt_list = expected_prompt

        while True:
            for item in prompt_list:
                if self._promptbuf[connection].rstrip(strip_chars).endswith(item.rstrip(strip_chars)):
                    return item, self._linebuf[connection]
                else:
                    time.sleep(.1)

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in InstrumentProtocol._get_raw_response()")

    def _do_cmd_resp(self, cmd, connection=None, *args, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param write_delay kwarg for the amount of delay in seconds to pause
        between each character. If none supplied, the DEFAULT_WRITE_DELAY
        value will be used.
        @param timeout optional wakeup and command timeout via kwargs.
        @param expected_prompt kwarg offering a specific prompt to look for
        other than the ones in the protocol class itself.
        @param response_regex kwarg with a compiled regex for the response to
        match. Groups that match will be returned as a string.
        Cannot be supplied with expected_prompt. May be helpful for
        instruments that do not have a prompt.
        @retval resp_result The (possibly parsed) response result including the
        first instance of the prompt matched. If a regex was used, the prompt
        will be an empty string and the response will be the joined collection
        of matched groups.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        expected_prompt = kwargs.get('expected_prompt', None)
        response_regex = kwargs.get('response_regex', None)
        write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)

        if response_regex and not isinstance(response_regex, RE_PATTERN):
            raise InstrumentProtocolException('Response regex is not a compiled pattern!')

        if expected_prompt and response_regex:
            raise InstrumentProtocolException('Cannot supply both regex and expected prompt!')

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not callable(build_handler):
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)

        cmd_line = build_handler(cmd, *args)
        # Wakeup the device, pass up exception if timeout

        self._wakeup(timeout, connection)

        # Clear line and prompt buffers for result.
        self._linebuf[connection] = ''
        self._promptbuf[connection] = ''

        # Send command.
        log.debug('_do_cmd_resp: %r, timeout=%s, write_delay=%s, expected_prompt=%r, response_regex=%r',
                  cmd_line, timeout, write_delay, expected_prompt, response_regex)

        self._send_data(cmd_line, write_delay, connection=connection)

        # Wait for the prompt, prepare result and return, timeout exception
        if response_regex:
            prompt = ""
            result_tuple = self._get_response(timeout,
                                              connection=connection,
                                              response_regex=response_regex,
                                              expected_prompt=expected_prompt)
            result = "".join(result_tuple)
        else:
            (prompt, result) = self._get_response(timeout,
                                                  expected_prompt=expected_prompt)

        resp_handler = self._response_handlers.get((self.get_current_state(), cmd),
                                                   self._response_handlers.get(cmd, None))
        resp_result = None
        if callable(resp_handler):
            resp_result = resp_handler(result, prompt)

        return resp_result

    def _send_data(self, data, write_delay=0, connection=None):
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        if write_delay == 0:
            self.connections[connection].send(data)
        else:
            for char in data:
                self.connections[connection].send(char)
                time.sleep(write_delay)

    def _do_cmd_no_resp(self, cmd, connection=None, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. No response is handled as a result of the command.

        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup timeout.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built.
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        timeout = kwargs.get('timeout', DEFAULT_CMD_TIMEOUT)
        write_delay = kwargs.get('write_delay', DEFAULT_WRITE_DELAY)

        build_handler = self._build_handlers.get(cmd, None)
        if not callable(build_handler):
            log.error('_do_cmd_no_resp: no handler for command: %s' % cmd)
            raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
        cmd_line = build_handler(cmd, *args)

        # Wakeup the device, timeout exception as needed
        self._wakeup(timeout, connection=connection)

        # Clear line and prompt buffers for result.

        self._linebuf[connection] = ''
        self._promptbuf[connection] = ''

        # Send command.
        self._send_data(cmd_line, write_delay, connection=connection)

    def _do_cmd_direct(self, cmd, connection=None):
        """
        Issue an untranslated command to the instrument. No response is handled
        as a result of the command.

        @param cmd The high level command to issue
        """
        # Send command.
        self._send_data(cmd, connection=connection)

    ########################################################################
    # Incoming data (for parsing) callback.
    ########################################################################
    def got_data(self, port_agent_packet, connection=None):
        """
        Called by the instrument connection when data is available.
        Append line and prompt buffers.

        Also add data to the chunker and when received call got_chunk
        to publish results.
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        data_length = port_agent_packet.get_data_length()
        data = port_agent_packet.get_data()
        timestamp = port_agent_packet.get_timestamp()

        log.debug("Got Data: %r %r", connection, data)
        log.debug("Add Port Agent Timestamp: %r %s", connection, timestamp)

        if data_length > 0:
            if self.get_current_state() == DriverProtocolState.DIRECT_ACCESS:
                self._driver_event(DriverAsyncEvent.DIRECT_ACCESS, data)

            self.add_to_buffer(data)

            self.chunkers[connection].add_chunk(data, timestamp)
            (timestamp, chunk) = self.chunkers[connection].get_next_data()
            while chunk:
                self._got_chunk(chunk, timestamp, connection=connection)
                (timestamp, chunk) = self.chunkers[connection].get_next_data()

    ########################################################################
    # Incoming raw data callback.
    ########################################################################
    def got_raw(self, port_agent_packet, connection=None):
        """
        Called by the port agent client when raw data is available, such as data
        sent by the driver to the instrument, the instrument responses,etc.
        """
        self.publish_raw(port_agent_packet)

    def publish_raw(self, port_agent_packet, connection=None):
        """
        Publish raw data
        @param: port_agent_packet port agent packet containing raw
        """
        particle = RawDataParticle(port_agent_packet.get_as_dict(),
                                   port_timestamp=port_agent_packet.get_timestamp())

        if self._driver_event:
            self._driver_event(DriverAsyncEvent.SAMPLE, particle.generate())

    def add_to_buffer(self, data, connection=None):
        """
        Add a chunk of data to the internal data buffers
        buffers implemented as lifo ring buffer
        @param data: bytes to add to the buffer
        """
        # Update the line and prompt buffers.
        self._linebuf[connection] += data
        self._promptbuf[connection] += data
        self._last_data_timestamp[connection] = time.time()

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        if len(self._linebuf[connection]) > self._max_buffer_size():
            self._linebuf[connection] = self._linebuf[connection][self._max_buffer_size() * -1:]

        # If our buffer exceeds the max allowable size then drop the leading
        # characters on the floor.
        if len(self._promptbuf[connection]) > self._max_buffer_size():
            self._promptbuf[connection] = self._promptbuf[connection][self._max_buffer_size() * -1:]

        log.debug("LINE BUF: %r", self._linebuf[-50:])
        log.debug("PROMPT BUF: %r", self._promptbuf[-50:])

    def _max_buffer_size(self):
        return MAX_BUFFER_SIZE

    def _got_chunk(self, chunk, timestamp, connection=None):
        pass

    ########################################################################
    # Wakeup helpers.
    ########################################################################

    def _send_wakeup(self, connection=None):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        pass

    def _wakeup(self, timeout, delay=1, connection=None):
        """
        Clear buffers and send a wakeup command to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        # Clear the prompt buffer.
        log.trace("clearing promptbuf: %r", self._promptbuf)
        self._promptbuf[connection] = ''

        # Grab time for timeout.
        starttime = time.time()

        while True:
            # Send a line return and wait a sec.
            log.trace('Sending wakeup. timeout=%s', timeout)
            self._send_wakeup(connection=connection)
            time.sleep(delay)

            log.trace("Prompts: %s", self._get_prompts())

            for item in self._get_prompts():
                log.trace("buffer: %r", self._promptbuf[connection])
                log.trace("find prompt: %r", item)
                index = self._promptbuf[connection].find(item)
                log.trace("Got prompt (index: %s): %r ", index, self._promptbuf[connection])
                if index >= 0:
                    log.trace('wakeup got prompt: %r', item)
                    return item
            log.trace("Searched for all prompts")

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException("in _wakeup()")

    def _wakeup_until(self, timeout, desired_prompt, delay=1, no_tries=5, connection=None):
        """
        Continue waking device until a specific prompt appears or a number
        of tries has occurred. Desired prompt must be in the instrument's
        prompt list.
        @param timeout The timeout to wake the device.
        @desired_prompt Continue waking until this prompt is seen.
        @delay Time to wake between consecutive wakeups.
        @no_tries Maximum number of wakeup tries to see desired prompt.
        @raises InstrumentTimeoutException if device could not be woken.
        @raises InstrumentProtocolException if the desired prompt is not seen in the
        maximum number of attempts.
        """
        if connection is None:
            raise InstrumentProtocolException('_get_response: no connection supplied!')

        count = 0
        while True:
            prompt = self._wakeup(timeout, delay, connection=connection)
            if prompt == desired_prompt:
                break
            else:
                time.sleep(delay)
                count += 1
                if count >= no_tries:
                    raise InstrumentProtocolException('Incorrect prompt.')
