import unittest

import pytz

from aggrolog import split_log_entry, parse_log_entry, to_local_time

class TestLogParsingMethods(unittest.TestCase):

    def test_quote_splits(self):
        source = 'FIRST "second" THIRD'

        result = split_log_entry(source)

        self.assertEqual(len(result), 3)
        self.assertEqual('second', result[1])

    def test_quotes_with_spaces(self):
        source = 'FIRST "second and a half" THIRD'

        result = split_log_entry(source)

        self.assertEqual(len(result), 3)
        self.assertEqual('second and a half', result[1])

    def test_block_splits(self):
        source = 'FIRST [second] THIRD'

        result = split_log_entry(source)

        self.assertEqual(len(result), 3)
        self.assertEqual('second', result[1])

    def test_blocks_with_spaces(self):
        source = 'FIRST [second and a half] THIRD'

        result = split_log_entry(source)

        self.assertEqual(len(result), 3)
        self.assertEqual('second and a half', result[1])

    def test_parse_log_to_dict(self):
        log = 'AAAAAAA sample.bucket [30/Nov/2018:00:27:57 +0000] 1.2.3.4 - BBBBBBB WEBSITE.GET.OBJECT index.html "GET / HTTP/1.1" 200 - 2784 2785 41 40 "-" "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" -'

        exp_bucket_owner = 'AAAAAAA'
        exp_bucket = 'sample.bucket'
        exp_time = '30/Nov/2018:00:27:57 +0000'
        exp_remote_ip = '1.2.3.4'
        exp_request_id = 'BBBBBBB'
        exp_operation = 'WEBSITE.GET.OBJECT'
        exp_key = "index.html"
        exp_request_uri = "GET / HTTP/1.1"
        exp_http_status = "200"
        exp_error_code = None
        exp_bytes_sent = "2784"
        exp_object_size = "2785"
        exp_total_time = "41"
        exp_turn_around_time = "40"
        exp_user_agent = "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

        result = parse_log_entry(log)

        self.assertEqual(exp_bucket_owner, result['bucket_owner'])
        self.assertEqual(exp_bucket, result['bucket'])
        self.assertEqual(exp_time, result['time'])
        self.assertEqual(exp_remote_ip, result['remote_ip'])
        self.assertIsNone(result['requester'])
        self.assertEqual(exp_request_id, result['request_id'])
        self.assertEqual(exp_operation, result['operation'])
        self.assertEqual(exp_key, result['key'])
        self.assertEqual(exp_request_uri, result['request_uri'])
        self.assertEqual(exp_http_status, result['http_status'])
        self.assertIsNone(exp_error_code, result['error_code'])
        self.assertEqual(exp_bytes_sent, result['bytes_sent'])
        self.assertEqual(exp_object_size, result['object_size'])
        self.assertEqual(exp_total_time, result['total_time'])
        self.assertEqual(exp_turn_around_time, result['turn_around_time'])
        self.assertIsNone(result['referrer'])
        self.assertEqual(exp_user_agent, result['user_agent'])
        self.assertIsNone(result['version_id'])

    def test_log_date_to_pst(self):
        log_date = '04/Jul/2018:01:19:20 +0000'

        result = to_local_time(log_date, pytz.timezone('America/Los_Angeles'))

        self.assertEqual(3, result.day)
        self.assertEqual(7, result.month)
        self.assertEqual(2018, result.year)
        self.assertEqual(18, result.hour)
        self.assertEqual(19, result.minute)
        self.assertEqual(20, result.second)


if __name__ == '__main__':
    unittest.main()