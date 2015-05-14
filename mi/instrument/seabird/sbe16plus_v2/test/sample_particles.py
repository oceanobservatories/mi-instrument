from mi.instrument.seabird.sbe16plus_v2.driver import NEWLINE

__author__ = 'rachelmanoni'

VALID_SAMPLE = "#0409DB0A738C81747A84AC0006000A2E541E18BE6ED9" + NEWLINE
VALID_SAMPLE2 = "0409DB0A738C81747A84AC0006000A2E541E18BE6ED9" + NEWLINE

VALID_GETSD_RESPONSE = "" + \
"<StatusData DeviceType = 'SBE19plus' SerialNumber = '01906914'>" + NEWLINE + \
"   <DateTime>2014-03-20T09:09:06</DateTime>" + NEWLINE + \
"   <LoggingState>not logging</LoggingState>" + NEWLINE + \
"   <EventSummary numEvents = '260'/>" + NEWLINE + \
"   <Power>" + NEWLINE + \
"      <vMain>13.0</vMain>" + NEWLINE + \
"      <vLith>8.6</vLith>" + NEWLINE + \
"      <iMain>51.1</iMain>" + NEWLINE + \
"      <iPump>145.6</iPump>" + NEWLINE + \
"      <iExt01> 0.5</iExt01>" + NEWLINE + \
"   </Power>" + NEWLINE + \
"   <MemorySummary>" + NEWLINE + \
"      <Bytes>330</Bytes>" + NEWLINE + \
"      <Samples>15</Samples>" + NEWLINE + \
"      <SamplesFree>2990809</SamplesFree>" + NEWLINE + \
"      <SampleLength>13</SampleLength>" + NEWLINE + \
"      <Profiles>0</Profiles>" + NEWLINE + \
"   </MemorySummary>" + NEWLINE + \
"</StatusData>" + NEWLINE

VALID_GETHD_RESPONSE = "<HardwareData DeviceType = 'SBE19plus' SerialNumber = '01906914'>" + NEWLINE + \
"   <Manufacturer>Sea-Bird Electronics, Inc.</Manufacturer>" + NEWLINE + \
"   <FirmwareVersion>2.5.2</FirmwareVersion>" + NEWLINE + \
"   <FirmwareDate>16 March 2011 08:50</FirmwareDate>" + NEWLINE + \
"   <CommandSetVersion>1.2</CommandSetVersion>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '49577' AssemblyNum = '41054H'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '46750' AssemblyNum = '41580B'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '49374' AssemblyNum = '41606'/>" + NEWLINE + \
"   <PCBAssembly PCBSerialNum = '38071' AssemblyNum = '41057A'/>" + NEWLINE + \
"   <MfgDate>29 SEP 2011</MfgDate>" + NEWLINE + \
"   <InternalSensors>" + NEWLINE + \
"      <Sensor id = 'Main Temperature'>" + NEWLINE + \
"         <type>temperature0</type>" + NEWLINE + \
"         <SerialNumber>01906914</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'Main Conductivity'>" + NEWLINE + \
"         <type>conductivity-0</type>" + NEWLINE + \
"         <SerialNumber>01906914</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'Main Pressure'>" + NEWLINE + \
"         <type>strain-0</type>" + NEWLINE + \
"         <SerialNumber>3313899</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"   </InternalSensors>" + NEWLINE + \
"   <ExternalSensors>" + NEWLINE + \
"      <Sensor id = 'volt 0'>" + NEWLINE + \
"         <type>SBE 43 OXY</type>" + NEWLINE + \
"         <SerialNumber>432484</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 1'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 2'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 3'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 4'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'volt 5'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"      <Sensor id = 'serial'>" + NEWLINE + \
"         <type>not assigned</type>" + NEWLINE + \
"         <SerialNumber>not assigned</SerialNumber>" + NEWLINE + \
"      </Sensor>" + NEWLINE + \
"   </ExternalSensors>" + NEWLINE + \
"</HardwareData>" + NEWLINE

VALID_GETCD_RESPONSE = "" + \
"<ConfigurationData DeviceType = 'SBE19plus' SerialNumber = '01906914'>" + NEWLINE + \
"   <ProfileMode>" + NEWLINE + \
"      <ScansToAverage>4</ScansToAverage>" + NEWLINE + \
"      <MinimumCondFreq>2500</MinimumCondFreq>" + NEWLINE + \
"      <PumpDelay>15</PumpDelay>" + NEWLINE + \
"      <AutoRun>no</AutoRun>" + NEWLINE + \
"      <IgnoreSwitch>yes</IgnoreSwitch>" + NEWLINE + \
"   </ProfileMode>" + NEWLINE + \
"   <Battery>" + NEWLINE + \
"      <Type>alkaline</Type>" + NEWLINE + \
"      <CutOff>7.5</CutOff>" + NEWLINE + \
"   </Battery>" + NEWLINE + \
"   <DataChannels>" + NEWLINE + \
"      <ExtVolt0>yes</ExtVolt0>" + NEWLINE + \
"      <ExtVolt1>no</ExtVolt1>" + NEWLINE + \
"      <ExtVolt2>no</ExtVolt2>" + NEWLINE + \
"      <ExtVolt3>no</ExtVolt3>" + NEWLINE + \
"      <ExtVolt4>no</ExtVolt4>" + NEWLINE + \
"      <ExtVolt5>no</ExtVolt5>" + NEWLINE + \
"      <SBE38>no</SBE38>" + NEWLINE + \
"      <WETLABS>no</WETLABS>" + NEWLINE + \
"      <OPTODE>no</OPTODE>" + NEWLINE + \
"      <SBE63>no</SBE63>" + NEWLINE + \
"      <SBE50>no</SBE50>" + NEWLINE + \
"      <GTD>no</GTD>" + NEWLINE + \
"   </DataChannels>" + NEWLINE + \
"   <EchoCharacters>yes</EchoCharacters>" + NEWLINE + \
"   <OutputExecutedTag>no</OutputExecutedTag>" + NEWLINE + \
"   <OutputFormat>raw HEX</OutputFormat>" + NEWLINE + \
"</ConfigurationData>" + NEWLINE

VALID_DCAL_STRAIN = "" + \
"<CalibrationCoefficients DeviceType = 'SBE19plus' SerialNumber = '01906914'>" + NEWLINE + \
"   <Calibration format = 'TEMP1' id = 'Main Temperature'>" + NEWLINE + \
"      <SerialNum>01906914</SerialNum>" + NEWLINE + \
"      <CalDate>09-Oct-11</CalDate>" + NEWLINE + \
"      <TA0>1.254755e-03</TA0>" + NEWLINE + \
"      <TA1>2.758871e-04</TA1>" + NEWLINE + \
"      <TA2>-1.368268e-06</TA2>" + NEWLINE + \
"      <TA3>1.910795e-07</TA3>" + NEWLINE + \
"      <TOFFSET>0.000000e+00</TOFFSET>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'WBCOND0' id = 'Main Conductivity'>" + NEWLINE + \
"      <SerialNum>01906914</SerialNum>" + NEWLINE + \
"      <CalDate>09-Oct-11</CalDate>" + NEWLINE + \
"      <G>-9.761799e-01</G>" + NEWLINE + \
"      <H>1.369994e-01</H>" + NEWLINE + \
"      <I>-3.523860e-04</I>" + NEWLINE + \
"      <J>4.404252e-05</J>" + NEWLINE + \
"      <CPCOR>-9.570000e-08</CPCOR>" + NEWLINE + \
"      <CTCOR>3.250000e-06</CTCOR>" + NEWLINE + \
"      <CSLOPE>1.000000e+00</CSLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'STRAIN0' id = 'Main Pressure'>" + NEWLINE + \
"      <SerialNum>3313899</SerialNum>" + NEWLINE + \
"      <CalDate>06-Oct-11</CalDate>" + NEWLINE + \
"      <PA0>-3.689246e-02</PA0>" + NEWLINE + \
"      <PA1>1.545570e-03</PA1>" + NEWLINE + \
"      <PA2>6.733197e-12</PA2>" + NEWLINE + \
"      <PTCA0>5.249034e+05</PTCA0>" + NEWLINE + \
"      <PTCA1>1.423189e+00</PTCA1>" + NEWLINE + \
"      <PTCA2>-1.206562e-01</PTCA2>" + NEWLINE + \
"      <PTCB0>2.501288e+01</PTCB0>" + NEWLINE + \
"      <PTCB1>-2.250000e-04</PTCB1>" + NEWLINE + \
"      <PTCB2>0.000000e+00</PTCB2>" + NEWLINE + \
"      <PTEMPA0>-5.677620e+01</PTEMPA0>" + NEWLINE + \
"      <PTEMPA1>5.424624e+01</PTEMPA1>" + NEWLINE + \
"      <PTEMPA2>-2.278113e-01</PTEMPA2>" + NEWLINE + \
"      <POFFSET>0.000000e+00</POFFSET>" + NEWLINE + \
"      <PRANGE>5.080000e+02</PRANGE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 0'>" + NEWLINE + \
"      <OFFSET>-4.650526e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246381e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 1'>" + NEWLINE + \
"      <OFFSET>-4.618105e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247197e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 2'>" + NEWLINE + \
"      <OFFSET>-4.659790e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247601e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 3'>" + NEWLINE + \
"      <OFFSET>-4.502421e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246911e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 4'>" + NEWLINE + \
"      <OFFSET>-4.589158e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246346e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 5'>" + NEWLINE + \
"      <OFFSET>-4.609895e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247868e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'FREQ0' id = 'external frequency channel'>" + NEWLINE + \
"      <EXTFREQSF>1.000008e+00</EXTFREQSF>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"</CalibrationCoefficients>" + NEWLINE

VALID_DCAL_QUARTZ = "" + \
"<CalibrationCoefficients DeviceType = 'SBE19plus' SerialNumber = '01906914'>" + NEWLINE + \
"   <Calibration format = 'TEMP1' id = 'Main Temperature'>" + NEWLINE + \
"      <SerialNum>01906914</SerialNum>" + NEWLINE + \
"      <CalDate>09-Oct-11</CalDate>" + NEWLINE + \
"      <TA0>1.254755e-03</TA0>" + NEWLINE + \
"      <TA1>2.758871e-04</TA1>" + NEWLINE + \
"      <TA2>-1.368268e-06</TA2>" + NEWLINE + \
"      <TA3>1.910795e-07</TA3>" + NEWLINE + \
"      <TOFFSET>0.000000e+00</TOFFSET>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'WBCOND0' id = 'Main Conductivity'>" + NEWLINE + \
"      <SerialNum>01906914</SerialNum>" + NEWLINE + \
"      <CalDate>09-Oct-11</CalDate>" + NEWLINE + \
"      <G>-9.761799e-01</G>" + NEWLINE + \
"      <H>1.369994e-01</H>" + NEWLINE + \
"      <I>-3.523860e-04</I>" + NEWLINE + \
"      <J>4.404252e-05</J>" + NEWLINE + \
"      <CPCOR>-9.570000e-08</CPCOR>" + NEWLINE + \
"      <CTCOR>3.250000e-06</CTCOR>" + NEWLINE + \
"      <CSLOPE>1.000000e+00</CSLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'STRAIN0' id = 'Main Pressure'>" + NEWLINE + \
"      <SerialNum>3313899</SerialNum>" + NEWLINE + \
"      <CalDate>06-Oct-11</CalDate>" + NEWLINE + \
"      <PC1>-4.642673e+03</PC1>" + NEWLINE + \
"      <PC2>-4.611640e-03</PC2>" + NEWLINE + \
"      <PC3>8.921190e-04</PC3>" + NEWLINE + \
"      <PD1>7.024800e-02</PD1>" + NEWLINE + \
"      <PD2>0.000000e+00</PD2>" + NEWLINE + \
"      <PT1>3.022595e+01</PT1>" + NEWLINE + \
"      <PT2>-1.549720e-04</PT2>" + NEWLINE + \
"      <PT3>2.677750e-06</PT3>" + NEWLINE + \
"      <PT4>1.705490e-09</PT4>" + NEWLINE + \
"      <PSLOPE>-1.000000e+00</PSLOPE>" + NEWLINE + \
"      <POFFSET>0.000000e+00</POFFSET>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 0'>" + NEWLINE + \
"      <OFFSET>-4.650526e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246381e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 1'>" + NEWLINE + \
"      <OFFSET>-4.618105e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247197e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 2'>" + NEWLINE + \
"      <OFFSET>-4.659790e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247601e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 3'>" + NEWLINE + \
"      <OFFSET>-4.502421e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246911e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 4'>" + NEWLINE + \
"      <OFFSET>-4.589158e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.246346e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'VOLT0' id = 'Volt 5'>" + NEWLINE + \
"      <OFFSET>-4.609895e-02</OFFSET>" + NEWLINE + \
"      <SLOPE>1.247868e+00</SLOPE>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"   <Calibration format = 'FREQ0' id = 'external frequency channel'>" + NEWLINE + \
"      <EXTFREQSF>1.000008e+00</EXTFREQSF>" + NEWLINE + \
"   </Calibration>" + NEWLINE + \
"</CalibrationCoefficients>" + NEWLINE


VALID_STATUS_RESPONSE = VALID_GETSD_RESPONSE + VALID_GETHD_RESPONSE + VALID_GETCD_RESPONSE