__author__ = 'mworden'


# A regex used to match any characters
ANY_CHARS_REGEX = r'.*'

# A regex used to match any characters
ANY_NON_SPACE_CHARS_REGEX = r'([^\s]*)'

# A regex used to match a single space
SPACE_REGEX = ' '

# A regex used to match the end of a line
END_OF_LINE_REGEX = r'(?:\r\n|\n)'

# A regex used to match a float value
FLOAT_REGEX = r'(?:[+-]?[0-9]|[1-9][0-9])+\.[0-9]+'

# A regex used to match a value in scientific notation
SCIENTIFIC_REGEX = r'([+-]?[0-9]\.[0-9]+)e([+-][0-9][0-9])'

# A regex used to match an int value
INT_REGEX = r'[+-]?[0-9]+'

# A regex used to match an unsigned int value
UNSIGNED_INT_REGEX = r'[0-9]+'

# A regex used to match against one or more tab characters
MULTIPLE_TAB_REGEX = r'\t+'

# A regex used to match against one or more whitespace characters
ONE_OR_MORE_WHITESPACE_REGEX = r'\s+'

# A regex to match ASCII-HEX characters
ASCII_HEX_CHAR_REGEX = r'[0-9A-Fa-f]'

# A regex used to match a date in the format YYYY/MM/DD
DATE_YYYY_MM_DD_REGEX = r'(\d{4})\/(\d{2})\/(\d{2})'

# A regex used to match a date in the format YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD and YYYY-MM, YYYY/MM and YYYYMM
DATE2_YYYY_MM_DD_REGEX = r'(\d{4})[-\/]?(\d{2})[-\/]?(\d{2})?'

# A regex used to match time in the format of HH:MM:SS.mmm
TIME_HR_MIN_SEC_MSEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})'

# A regex used to match a date in the format MM/DD/YYYY
DATE_MM_DD_YYYY_REGEX = r'(\d{2})/(\d{2})/(\d{4})'

# A regex used to match time in the format of HH:MM:SS
TIME_HR_MIN_SEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})'

# A regex for a common three character month abbreviation
THREE_CHAR_MONTH_REGEX = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'

# A regex for a common three character day of week abbreviation
THREE_CHAR_DAY_OF_WEEK_REGEX = r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)'

# Date related regex patterns
DATE_DAY_REGEX = '\d{2}'
DATE_YEAR_REGEX = '\d{4}'

DCL_TIMESTAMP_REGEX = '^' + DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX

