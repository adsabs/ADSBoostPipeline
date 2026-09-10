#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for writing boost factors to the database.

These run against a real (sqlite) database rather than a mock, so they cover
the batched insert/update path that handle_message_payload now uses.
"""

import pytest
import os
import tempfile
import shutil
from adsboost import models
from adsboost.app import ADSBoostCelery


def make_factors(**overrides):
    """A full set of boost factors, as compute_final_boost returns them"""
    factors = {
        'refereed_boost': 1.0,
        'doctype_boost': 1.0,
        'recency_boost': 0.5,
        'boost_factor': 0.9,
        'astrophysics_weight': 1.0,
        'physics_weight': 0.64,
        'earthscience_weight': 0.1,
        'planetary_weight': 0.46,
        'heliophysics_weight': 0.28,
        'general_weight': 0.64,
        'astrophysics_final_boost': 0.9,
        'physics_final_boost': 0.576,
        'earthscience_final_boost': 0.09,
        'planetary_final_boost': 0.414,
        'heliophysics_final_boost': 0.252,
        'general_final_boost': 0.576,
    }
    factors.update(overrides)
    return factors


class TestStoreBoostFactors:
    """Test store_boost_factors and store_boost_factors_batch against a database"""

    @pytest.fixture
    def app(self):
        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, 'test_boost.db')
        app = ADSBoostCelery(
            'ADSBoostPipeline',
            local_config={'SQLALCHEMY_URL': 'sqlite:///' + db_path})
        models.Base.metadata.create_all(app._engine)
        yield app
        shutil.rmtree(tmpdir, ignore_errors=True)

    def fetch(self, app, bibcode):
        with app.session_scope() as session:
            record = session.query(models.BoostFactors).filter_by(bibcode=bibcode).first()
            if record is None:
                return None
            return {c: getattr(record, c) for c in app.BOOST_FACTOR_COLUMNS}

    def test_boost_factor_column_is_written(self, app):
        """boost_factor is computed and must actually reach the database"""
        app.store_boost_factors('2022ApJ...931...44P', 'scix:AAAA-BBBB-CCCC',
                                make_factors(boost_factor=0.75))

        stored = self.fetch(app, '2022ApJ...931...44P')
        assert stored['boost_factor'] == 0.75

    def test_every_column_is_written(self, app):
        """No boost factor should be silently dropped on the way to the database"""
        factors = make_factors()
        app.store_boost_factors('2022ApJ...931...44P', 'scix:AAAA-BBBB-CCCC', factors)

        stored = self.fetch(app, '2022ApJ...931...44P')
        for column in app.BOOST_FACTOR_COLUMNS:
            assert stored[column] == factors[column], column

    def test_batch_inserts_every_record(self, app):
        entries = [
            {'bibcode': 'BIB%04d' % i, 'scix_id': 'scix:%04d' % i,
             'boost_factors': make_factors(boost_factor=i / 10.0)}
            for i in range(10)
        ]

        app.store_boost_factors_batch(entries)

        with app.session_scope() as session:
            assert session.query(models.BoostFactors).count() == 10
        assert self.fetch(app, 'BIB0007')['boost_factor'] == 0.7

    def test_batch_updates_existing_records_in_place(self, app):
        """A second batch for the same bibcodes must update, not duplicate"""
        entries = [
            {'bibcode': 'BIB0001', 'scix_id': 'scix:0001',
             'boost_factors': make_factors(boost_factor=0.1)},
            {'bibcode': 'BIB0002', 'scix_id': 'scix:0002',
             'boost_factors': make_factors(boost_factor=0.2)},
        ]
        app.store_boost_factors_batch(entries)

        entries[0]['boost_factors'] = make_factors(boost_factor=0.9)
        entries[1]['boost_factors'] = make_factors(boost_factor=0.8)
        app.store_boost_factors_batch(entries)

        with app.session_scope() as session:
            assert session.query(models.BoostFactors).count() == 2
        assert self.fetch(app, 'BIB0001')['boost_factor'] == 0.9
        assert self.fetch(app, 'BIB0002')['boost_factor'] == 0.8

    def test_duplicate_bibcode_within_one_batch_does_not_insert_twice(self, app):
        """Master can send the same bibcode twice in one list"""
        app.store_boost_factors_batch([
            {'bibcode': 'BIB0001', 'scix_id': 'scix:0001',
             'boost_factors': make_factors(boost_factor=0.1)},
            {'bibcode': 'BIB0001', 'scix_id': 'scix:0001',
             'boost_factors': make_factors(boost_factor=0.5)},
        ])

        with app.session_scope() as session:
            assert session.query(models.BoostFactors).count() == 1
        assert self.fetch(app, 'BIB0001')['boost_factor'] == 0.5

    def test_batch_matches_one_at_a_time(self, app):
        """Batching must not change what ends up in the database"""
        factors = make_factors(boost_factor=0.42)
        app.store_boost_factors('BIB0001', 'scix:0001', factors)
        app.store_boost_factors_batch([
            {'bibcode': 'BIB0002', 'scix_id': 'scix:0002', 'boost_factors': factors}
        ])

        assert self.fetch(app, 'BIB0001') == self.fetch(app, 'BIB0002')

    def test_empty_batch_is_a_no_op(self, app):
        app.store_boost_factors_batch([])

        with app.session_scope() as session:
            assert session.query(models.BoostFactors).count() == 0
