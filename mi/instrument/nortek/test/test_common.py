from unittest import TestCase

from nose.plugins.attrib import attr

from mi.core.exceptions import SampleException
from mi.instrument.nortek.common import (convert_word_to_int, convert_word_to_bit_field,
                                         convert_bcd_bytes_to_ints, convert_time)


@attr('UNIT', group='mi')
class CommonTest(TestCase):
    def test_convert_word_to_int(self):
        word = '\x00\x00'
        value = convert_word_to_int(word)
        self.assertEqual(value, 0)

        word = '\x00\x01'
        value = convert_word_to_int(word)
        self.assertEqual(value, 256)

        word = '\x01\x00'
        value = convert_word_to_int(word)
        self.assertEqual(value, 1)

        word = '\x01\x01'
        value = convert_word_to_int(word)
        self.assertEqual(value, 257)

        word = '\xff\xff'
        value = convert_word_to_int(word)
        self.assertEqual(value, 65535)

        word = '\x00\x00\x00'
        with self.assertRaises(SampleException):
            convert_word_to_int(word)

    def test_convert_bytes_to_bit_field(self):
        word = '\x00\x00'
        value = convert_word_to_bit_field(word)
        self.assertEqual(value, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        word = '\x01\x00'
        value = convert_word_to_bit_field(word)
        self.assertEqual(value, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

        word = '\x02\x00'
        value = convert_word_to_bit_field(word)
        self.assertEqual(value, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0])

        word = '\x00\x01'
        value = convert_word_to_bit_field(word)
        self.assertEqual(value, [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0])

        word = '\x00\x02'
        value = convert_word_to_bit_field(word)
        self.assertEqual(value, [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        word = '\x00\x00\x00'
        with self.assertRaises(SampleException):
            convert_word_to_bit_field(word)

    def test_convert_words_to_datetime(self):
        bytes = '\x01\x02\x03\x04\x05\x06'
        value = convert_bcd_bytes_to_ints(bytes)
        self.assertEqual(value, [1, 2, 3, 4, 5, 6])

        bytes = '\x12\x13\x14\x15\x16\x17'
        value = convert_bcd_bytes_to_ints(bytes)
        self.assertEqual(value, [12, 13, 14, 15, 16, 17])

        bytes = '\x10\x20\x30\x40\x50\x60'
        value = convert_bcd_bytes_to_ints(bytes)
        self.assertEqual(value, [10, 20, 30, 40, 50, 60])

        bytes = '\x00\x00\x00'
        with self.assertRaises(SampleException):
            convert_bcd_bytes_to_ints(bytes)

    def test_convert_time(self):
        bytes = '\x05\x06\x03\x04\x01\x02'
        result = convert_time(bytes)
        self.assertEqual(2001, result.year)
        self.assertEqual(2, result.month)
        self.assertEqual(3, result.day)
        self.assertEqual(4, result.hour)
        self.assertEqual(5, result.minute)
        self.assertEqual(6, result.second)
