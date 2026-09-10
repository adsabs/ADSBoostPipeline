#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the message handling path in app.py.

Testing for BoostRequestRecordList (batched) messages as well as single
BoostRequestRecord messages, going through the adsmsg celery serializer used by
RabbitMQ to send messagesbetween Master Pipeline and Boost Pipeline.
"""

import pytest
import json
import os
from adsmsg import BoostRequestRecord, BoostRequestRecordList, BoostResponseRecordList
from adsmsg.protobuf import status_pb2 as Status
from adsputils.serializer import dumps as adsmsg_dumps, loads as adsmsg_loads
from adsboost.app import ADSBoostCelery
from adsputils import load_config, setup_logging

# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('test_message_handling.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'DEBUG'),
                       attach_stdout=config.get('LOG_STDOUT', False))


def encode_bytes_field(value):
    """Mirror of Master Pipeline's _encode_boost_bytes_field."""
    if value is None or value == '':
        return b''
    if isinstance(value, bytes):
        return value
    if isinstance(value, dict):
        return json.dumps(value).encode('utf-8')
    if isinstance(value, str):
        return value.encode('utf-8')
    return str(value).encode('utf-8')


def populate_entry(entry, record):
    """Mirror of Master Pipeline's _populate_boost_request_entry."""
    entry.bibcode = record.get('bibcode', '')
    entry.scix_id = record.get('scix_id', '')
    entry.status = Status.updated
    entry.bib_data = encode_bytes_field(record.get('bib_data', ''))
    entry.metrics = encode_bytes_field(record.get('metrics', ''))
    classifications = record.get('classifications') or []
    if classifications:
        entry.classifications.extend(classifications)


def build_request_list(records):
    """Build the BoostRequestRecordList that Master Pipeline sends."""
    message = BoostRequestRecordList()
    for record in records:
        populate_entry(message.boost_requests.add(), record)
    message.status = Status.updated
    return message


def build_single_request(record):
    """Build the single BoostRequestRecord that Master Pipeline used to send."""
    message = BoostRequestRecord()
    populate_entry(message, record)
    return message


def through_broker(message):
    """Round-trip a message through the adsmsg celery serializer.

    forward_message publishes with the 'adsmsg' serializer, so this is the
    exact object shape a Boost celery worker receives off the queue.
    """
    body = adsmsg_dumps({'args': [message], 'kwargs': {}})
    return adsmsg_loads(body)['args'][0]


class TestMessageHandling:
    """Test handle_message_payload for list and single messages"""

    @pytest.fixture
    def app(self):
        return ADSBoostCelery('ADSBoostPipeline')

    @pytest.fixture
    def calls(self, app):
        """Capture stored/forwarded records without a database or broker"""
        stored = []
        forwarded = []
        app.store_boost_factors_batch = lambda entries: stored.append(entries)
        app.forward_message = lambda message, **kwargs: forwarded.append(message)
        return {'stored': stored, 'forwarded': forwarded}

    @pytest.fixture
    def records(self):
        """Load the stubdata records used elsewhere in the suite"""
        inputs_dir = os.path.join(os.path.dirname(__file__), 'stubdata', 'inputs')
        records = []
        for filename in sorted(os.listdir(inputs_dir)):
            if filename.endswith('.json'):
                with open(os.path.join(inputs_dir, filename)) as f:
                    records.append(json.load(f))
        return records

    def test_protobuf_to_dict_decodes_every_record_in_list(self, app, records):
        """bib_data/metrics are protobuf bytes fields, so MessageToDict base64
        encodes them; every entry in the list must come back decoded."""
        message = through_broker(build_request_list(records))

        result = app._protobuf_to_dict(message)

        assert 'boost_requests' in result
        assert len(result['boost_requests']) == len(records)
        for entry, original in zip(result['boost_requests'], records):
            assert entry['bibcode'] == original['bibcode']
            assert isinstance(entry['bib_data'], dict)
            assert isinstance(entry['metrics'], dict)
            assert entry['bib_data']['doctype'] == original['bib_data']['doctype']

    def test_list_message_processes_every_record(self, app, calls, records):
        """A BoostRequestRecordList of N records is stored and answered as a batch"""
        message = through_broker(build_request_list(records))

        result = app.handle_message_payload(message=message)

        assert result['total'] == len(records)
        assert result['processed'] == len(records)
        assert result['failed'] == 0
        assert result['retried'] == 0
        expected = [r['bibcode'] for r in records]

        # one store_boost_factors_batch call carrying every record
        assert len(calls['stored']) == 1
        assert [e['bibcode'] for e in calls['stored'][0]] == expected

        # one BoostResponseRecordList carrying every record
        assert len(calls['forwarded']) == 1
        response = calls['forwarded'][0]
        assert isinstance(response, BoostResponseRecordList)
        assert [r.bibcode for r in response.boost_responses] == expected

    def test_batch_response_carries_computed_values(self, app, calls, records):
        """The batched response must carry the same numbers that were stored"""
        app.handle_message_payload(message=through_broker(build_request_list(records)))

        stored = {e['bibcode']: e['boost_factors'] for e in calls['stored'][0]}
        for response in calls['forwarded'][0].boost_responses:
            factors = stored[response.bibcode]
            assert response.boost_factor == pytest.approx(factors['boost_factor'])
            assert response.doctype_boost == pytest.approx(factors['doctype_boost'])
            assert response.refereed_boost == pytest.approx(factors['refereed_boost'])
            # protobuf uses the public collection names, the model uses internal ones
            assert response.astronomy_final_boost == \
                pytest.approx(factors['astrophysics_final_boost'])
            assert response.earth_science_final_boost == \
                pytest.approx(factors['earthscience_final_boost'])
            assert response.planetary_science_final_boost == \
                pytest.approx(factors['planetary_final_boost'])

    def test_single_message_still_processed(self, app, calls, records):
        """Backward compatibility: a single BoostRequestRecord still works"""
        message = through_broker(build_single_request(records[0]))

        app.handle_message_payload(message=message)

        assert len(calls['stored']) == 1
        assert calls['stored'][0][0]['bibcode'] == records[0]['bibcode']
        assert len(calls['forwarded']) == 1

    def test_list_and_single_produce_identical_boost_factors(self, app, records):
        """Batching must not change the computed values"""
        record = records[0]

        single = app._protobuf_to_dict(through_broker(build_single_request(record)))
        batched = app._protobuf_to_dict(
            through_broker(build_request_list([record])))['boost_requests'][0]

        assert app.compute_final_boost(app._parse_master_pipeline_message(single)) == \
            app.compute_final_boost(app._parse_master_pipeline_message(batched))

    def test_one_bad_record_does_not_drop_the_batch(self, app, calls, records):
        """A record that fails to parse must not stop the rest of the list"""
        message = BoostRequestRecordList()
        populate_entry(message.boost_requests.add(), records[0])
        bad = message.boost_requests.add()
        bad.bibcode = 'BAD_RECORD'
        bad.status = Status.updated
        bad.bib_data = b'not valid json {{{'
        populate_entry(message.boost_requests.add(), records[1])
        message.status = Status.updated

        app.handle_message_payload(message=through_broker(message))

        bibcodes = [e['bibcode'] for e in calls['stored'][0]]
        assert records[0]['bibcode'] in bibcodes
        assert records[1]['bibcode'] in bibcodes
        # the good records are still answered, in one response message
        assert len(calls['forwarded']) == 1

    def test_record_without_bibcode_is_skipped(self, app, calls, records):
        """No bibcode means nothing to key on, so the entry is dropped"""
        message = BoostRequestRecordList()
        entry = message.boost_requests.add()
        entry.scix_id = 'scix:AAAA-BBBB-CCCC'
        entry.status = Status.updated
        entry.bib_data = encode_bytes_field(records[0]['bib_data'])
        message.status = Status.updated

        app.handle_message_payload(message=through_broker(message))

        assert calls['stored'] == []
        assert calls['forwarded'] == []

    def test_empty_bytes_fields_are_tolerated(self, app, calls):
        """Master sends b'' when a record has no bib_data or metrics"""
        message = BoostRequestRecordList()
        entry = message.boost_requests.add()
        entry.bibcode = '2022ApJ...931...44P'
        entry.status = Status.updated
        entry.bib_data = b''
        entry.metrics = b''
        message.status = Status.updated

        app.handle_message_payload(message=through_broker(message))

        assert len(calls['stored']) == 1

    def test_empty_list_is_a_no_op(self, app, calls):
        """An empty list must not raise"""
        message = BoostRequestRecordList()
        message.status = Status.updated

        app.handle_message_payload(message=through_broker(message))

        assert calls['stored'] == []
        assert calls['forwarded'] == []

    def test_dict_payload_with_boost_requests_key(self, app, calls, records):
        """run.py and the tests hand in plain dicts rather than protobufs"""
        result = app.handle_message_payload(message={'boost_requests': records})

        assert result['processed'] == len(records)
        assert len(calls['stored'][0]) == len(records)

    def test_json_string_payload_still_supported(self, app, calls, records):
        """Celery can also deliver a plain JSON string"""
        app.handle_message_payload(message=json.dumps(records[0]))

        assert len(calls['stored']) == 1
        assert calls['stored'][0][0]['bibcode'] == records[0]['bibcode']

    def test_transient_failure_is_retried_once(self, app, calls, records):
        """A record that fails once then succeeds must end up processed"""
        real = app.compute_boost_request
        seen = {'calls': 0}

        def flaky(request):
            # fail the first record exactly once, then behave normally
            if request.get('bibcode') == records[0]['bibcode'] and seen['calls'] == 0:
                seen['calls'] += 1
                raise RuntimeError("transient")
            return real(request)

        app.compute_boost_request = flaky
        result = app.handle_message_payload(
            message=through_broker(build_request_list(records)))

        assert result['retried'] == 1
        assert result['failed'] == 0
        assert result['processed'] == len(records)
        # the retried record still made it into the batch
        assert records[0]['bibcode'] in [e['bibcode'] for e in calls['stored'][0]]

    def test_permanent_failure_counts_as_failed_after_retry(self, app, calls, records):
        """A record that always fails is retried once, then given up on"""
        real = app.compute_boost_request
        attempts = {'n': 0}

        def always_fails(request):
            if request.get('bibcode') == records[0]['bibcode']:
                attempts['n'] += 1
                raise RuntimeError("permanent")
            return real(request)

        app.compute_boost_request = always_fails
        result = app.handle_message_payload(
            message=through_broker(build_request_list(records)))

        assert attempts['n'] == 2          # original + exactly one retry
        assert result['failed'] == 1
        assert result['processed'] == len(records) - 1
